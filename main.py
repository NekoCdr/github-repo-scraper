import logging
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.exceptions import TransportServerError

db_path = Path("github_statistics.db")

con = sqlite3.connect(db_path)
con.execute("PRAGMA foreign_keys = ON")

now_str = datetime.now().strftime("%Y_%m_%d__%H_%M_%S")

log_path = Path(f"logs/main_{now_str}.log")
log_path.parent.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger(__name__)
logging.basicConfig(filename=log_path, level=logging.INFO)

PRS_PER_REQUEST = 10
RELATED_ENTITIES_PER_REQUEST = 5
COST_PER_MINUTE = 1900 # GH limit is 2000

def prepareQuery(after: str | None = None):
    query = gql(
        """
        query (
            $owner: String!
            $name: String!
            $prs_limit: Int!
            $entities_limit: Int!
            $after: String
        ){
            rateLimit {
                cost
                limit
                remaining
                used
                resetAt
            }
            repository(
                owner: $owner
                name: $name
            ) {
                pullRequests(
                    labels: ["clang"]
                    first: $prs_limit
                    after: $after
                    # states: [OPEN, CLOSED]
                    orderBy: {field: CREATED_AT, direction: ASC}
                ){
                    nodes{
                        number # int
                        author{
                            login # string
                        }
                        title # string
                        state # string enum: CLOSED, MERGED, OPEN
                        createdAt # datetime string, for example "2023-08-31T12:16:25Z"
                        closedAt # datetime string
                        mergedAt # datetime string
                        additions # int, added lines
                        deletions # int, deleted lines

                        # count commits 
                        commits(first: $entities_limit){
                            nodes{
                                commit{
                                    commitUrl # string
                                    committedDate # datetime string
                                }
                            }
                        }

                        # count comments
                        comments(
                            first: $entities_limit
                        ){
                            nodes{
                                fullDatabaseId # bigint (string numeric), nullable
                                author{
                                    login # string
                                }
                                createdAt # datetime string
                            }
                        }

                        # count reviews
                        reviews(
                            first: $entities_limit
                        ){
                            nodes{
                                author{
                                    login
                                }
                                createdAt
                                fullDatabaseId
                                comments(first: $entities_limit) {
                                    nodes{
                                        fullDatabaseId # bigint (string numeric), nullable
                                        author{
                                            login # string
                                        }
                                        createdAt # datetime string
                                    }
                                }
                            }
                        }

                        reviewThreads(first: $entities_limit){
                            nodes{
                                id
                                comments(first: $entities_limit) {
                                    nodes{
                                        fullDatabaseId # bigint (string numeric), nullable
                                        author{
                                            login # string
                                        }
                                        createdAt # datetime string
                                    }
                                }
                            }
                        }

                        changedFiles # int
                        files(first: $entities_limit){
                            nodes{
                                path # string
                                changeType # string enum: ADDED, CHANGED, COPIED, DELETED, MODIFIED, RENAMED
                                additions # int
                                deletions # int
                            }
                        }

                        # count participants
                        participants(first: $entities_limit) {
                            nodes{
                                login # string
                                name # string, nullable
                            }
                        }

                        authorAssociation # string enum: COLLABORATOR, CONTRIBUTOR, FIRST_TIMER, FIRST_TIME_CONTRIBUTOR, MANNEQUIN, MEMBER, NONE, OWNER
                        headRepository {
                            url # string (scalar URI)
                        }
                        isCrossRepository # bool

                        labels(first: $entities_limit) {
                            nodes{
                                name # string
                            }
                        }

                        mergeCommit{
                            statusCheckRollup{
                                state # string enum: ERROR, EXPECTED, FAILURE, PENDING, SUCCESS
                            }
                        }

                        totalCommentsCount # int, comments + review comments + thread comments
                    }
                    pageInfo{
                        startCursor
                        endCursor
                        hasNextPage
                        hasPreviousPage
                    }
                }
            }
        }
        """
    )

    query.variable_values = {
        "owner": "llvm",
        "name": "llvm-project",
        "prs_limit": PRS_PER_REQUEST,
        "entities_limit": RELATED_ENTITIES_PER_REQUEST,
        "after": after,
    }

    return query

def executeQuery(query, token: str | None = None):
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    transport = AIOHTTPTransport(url="https://api.github.com/graphql", headers=headers)
    client = Client(transport=transport)
    return client.execute(query)

def saveAuthor(login: str, name: str | None = None):
    con.execute(
        """
        insert into authors (login, name)
        values (?, ?)
        on conflict (login) do update set name = excluded.name
        where excluded.name is not null
        """,
        (login, name)
    )

def saveAuthors(authors: list | None):
    if authors is None or not len(authors):
        return

    query_data = []
    for author in authors:
        query_data.append((
            author.get("login"),
            author.get("name"),
        ))
    con.executemany(
        """
        insert into authors (login, name)
        values (?, ?)
        on conflict (login) do update set name = excluded.name
        where excluded.name is not null
        """,
        query_data
    )
    con.commit()

def getAuthorIDByLogin(login: str) -> int | None:
    author = con.execute("select id from authors where login like ?", (login,)).fetchone()
    return None if author is None else author[0]

def saveCommits(commits: list | None, pull_request_id: int):
    if commits is None or not len(commits):
        return

    query_data = []
    for commit in commits:
        query_data.append((
            pull_request_id,
            (commit.get("commit") or {}).get("commitUrl"),
            (commit.get("commit") or {}).get("committedDate"),
        ))
    con.executemany(
        """
        insert into commits (
            pull_request_id,
            url,
            committed_at
        ) values (?, ?, ?)
        on conflict (pull_request_id, url) do nothing
        """,
        query_data
    )
    con.commit()

def saveComments(comments: list | None, pull_request_id: int | None = None, review_id: int | None = None, review_thread_id: int | None = None):
    if comments is None or not len(comments):
        return

    query_data = []
    for comment in comments:
        author_id: int | None = None
        author_login: str | None = (comment.get("author") or {}).get("login")
        if author_login is not None:
            saveAuthor(author_login)
            author_id = getAuthorIDByLogin(author_login)

        query_data.append((
            int(comment.get("fullDatabaseId")),
            author_id,
            pull_request_id,
            review_id,
            review_thread_id,
            comment.get("createdAt"),
        ))
    con.executemany(
        """
        insert into comments (
            id,
            author_id,
            pull_request_id,
            review_id,
            review_thread_id,
            created_at
        ) values(?, ?, ?, ?, ?, ?)
        on conflict (id) do update set
            pull_request_id  = coalesce(excluded.pull_request_id,  comments.pull_request_id),
            review_id        = coalesce(excluded.review_id,        comments.review_id),
            review_thread_id = coalesce(excluded.review_thread_id, comments.review_thread_id)
        """,
        query_data
    )
    con.commit()

def saveReviews(reviews: list | None, pull_request_id: int):
    if reviews is None or not len(reviews):
        return

    for review in reviews:
        review_id: int = int(review.get("fullDatabaseId"))

        author_id: int | None = None
        author_login: str | None = (review.get("author") or {}).get("login")
        if author_login is not None:
            saveAuthor(author_login)
            author_id = getAuthorIDByLogin(author_login)

        con.execute(
            """
            insert into reviews (
                id,
                author_id,
                pull_request_id,
                created_at
            ) values(?, ?, ?, ?)
            on conflict (id) do nothing
            """,
            (
                review_id,
                author_id,
                pull_request_id,
                review.get("createdAt"),
            )
        )

        saveComments((review.get("comments") or {}).get("nodes"), review_id=review_id)

def saveReviewThreads(threads: list | None, pull_request_id: int):
    if threads is None or not len(threads):
        return

    for thread in threads:
        node_id: int = thread.get("id")

        cur = con.execute(
            """
            insert into review_threads (node_id, pull_request_id)
            values(?, ?)
            on conflict (node_id) do update set node_id = excluded.node_id
            returning id
            """,
            (
                node_id,
                pull_request_id,
            )
        )
        res = cur.fetchone()
        thread_id = None if res is None else res[0]

        saveComments((thread.get("comments") or {}).get("nodes"), review_thread_id=thread_id)

def saveFiles(files: list | None, pull_request_id: int):
    if files is None or not len(files):
        return

    query_data = []
    for file in files:
        query_data.append((
            pull_request_id,
            file.get("path"),
            file.get("changeType"),
            file.get("additions"),
            file.get("deletions"),
        ))
    con.executemany(
        """
        insert into files (
            pull_request_id,
            path,
            change_type,
            additions,
            deletions
        ) values (?, ?, ?, ?, ?)
        on conflict (pull_request_id, path) do nothing
        """,
        query_data
    )
    con.commit()

def saveParticipants(authors: list | None, pull_request_id: int):
    if authors is None or not len(authors):
        return
    
    saveAuthors(authors)

    query_data = []
    for author in authors:
        author_id: int | None = None
        author_login: str | None = author.get("login")
        
        if author_login is not None:
            author_id = getAuthorIDByLogin(author_login)
        if author_id is None:
            continue
        
        query_data.append((
            author_id,
            pull_request_id,
        ))
    con.executemany(
        """
        insert into author_pull_request (author_id, pull_request_id)
        values (?, ?)
        on conflict (author_id, pull_request_id) do nothing
        """,
        query_data
    )
    con.commit()

def saveLabels(labels: list | None):
    if labels is None or not len(labels):
        return

    query_data = []
    for label in labels:
        query_data.append((
            label.get("name"),
        ))
    con.executemany(
        """
        insert into labels (name)
        values (?)
        on conflict (name) do nothing
        """,
        query_data
    )
    con.commit()

def getLabelIDByName(name: str) -> int | None:
    label = con.execute("select id from labels where name like ?", (name,)).fetchone()
    return None if label is None else label[0]

def linkLabelsWithPullRequest(labels: list | None, pull_request_id: int):
    if labels is None or not len(labels):
        return
    
    saveLabels(labels)

    query_data = []
    for label in labels:
        label_id: int | None = None
        label_name: str | None = label.get("name")
        
        if label_name is not None:
            label_id = getLabelIDByName(label_name)
        if label_id is None:
            continue
        
        query_data.append((
            label_id,
            pull_request_id,
        ))
    con.executemany(
        """
        insert into label_pull_request (label_id, pull_request_id)
        values (?, ?)
        on conflict (label_id, pull_request_id) do nothing
        """,
        query_data
    )
    con.commit()

def savePullRequests(pull_requests: list):
    if not len(pull_requests):
        return

    for pull_request in pull_requests:
        pull_request_id: int = int(pull_request.get("number"))

        author_id: int | None = None
        author_login: str | None = (pull_request.get("author") or {}).get("login")
        if author_login is not None:
            saveAuthor(author_login)
            author_id = getAuthorIDByLogin(author_login)

        con.execute(
            """
            insert into pull_requests (
                id,
                author_id,
                title,
                state,
                created_at,
                closed_at,
                merged_at,
                additions,
                deletions,
                author_association,
                head_repository_url,
                is_cross_repository,
                merge_commit_ci_state,
                api_total_comments_count
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            on conflict (id) do update set
                author_id = excluded.author_id,
                title = excluded.title,
                state = excluded.state,
                created_at = excluded.created_at,
                closed_at = excluded.closed_at,
                merged_at = excluded.merged_at,
                additions = excluded.additions,
                deletions = excluded.deletions,
                author_association = excluded.author_association,
                head_repository_url = excluded.head_repository_url,
                is_cross_repository = excluded.is_cross_repository,
                merge_commit_ci_state = excluded.merge_commit_ci_state,
                api_total_comments_count = excluded.api_total_comments_count
            """,
            (
                pull_request_id,
                author_id,
                pull_request.get("title"),
                pull_request.get("state"),
                pull_request.get("createdAt"),
                pull_request.get("closedAt"),
                pull_request.get("mergedAt"),
                int(pull_request.get("additions")),
                int(pull_request.get("deletions")),
                pull_request.get("authorAssociation"),
                (pull_request.get("headRepository") or {}).get("url"),
                int(pull_request.get("isCrossRepository")),
                ((pull_request.get("mergeCommit") or {}).get("statusCheckRollup") or {}).get("state"),
                pull_request.get("totalCommentsCount")
            )
        )
        con.commit()

        saveCommits((pull_request.get("commits") or {}).get("nodes"), pull_request_id)
        saveComments((pull_request.get("comments") or {}).get("nodes"), pull_request_id)
        saveReviews((pull_request.get("reviews") or {}).get("nodes"), pull_request_id)
        saveReviewThreads((pull_request.get("reviewThreads") or {}).get("nodes"), pull_request_id)
        saveFiles((pull_request.get("files") or {}).get("nodes"), pull_request_id)
        saveParticipants((pull_request.get("participants") or {}).get("nodes"), pull_request_id)
        linkLabelsWithPullRequest((pull_request.get("labels") or {}).get("nodes"), pull_request_id)

def safeExit():
    con.close()
    exit()

token_pool = (
    "",
)
current_token_idx = 0
current_cost = 0

logger.info("Start executing")
print("Start executing")

has_next_page = True
next_page_after: str | None = None

start: float = time.perf_counter()
saved_pull_requests: int = 0

while has_next_page:
# for i in (1,2,3,4,5,6,7,8,9,10):
    try:
        if current_cost >= COST_PER_MINUTE:
            cur: float = time.perf_counter()
            cooldown: float = 60 - (cur - start)
            if cooldown > 0:
                print(f"=== sleep for {cooldown:.2f} seconds ===")
                time.sleep(cooldown)
            current_cost = 0
            start = time.perf_counter()

        q = prepareQuery(after=next_page_after)
        result = executeQuery(q, token_pool[current_token_idx])

        if "repository" not in result or "rateLimit" not in result:
            if "message" not in result:
                print("Unexpected GitHub answer. Check the log for more details")
                logger.error(repr(result))
                safeExit()
            print(f"GitHub error: {result.message}. Check the log for more details")
            logger.error(repr(result))
            safeExit()

        rate_limit = result["rateLimit"]

        current_cost += rate_limit["cost"]

        page_info = result["repository"]["pullRequests"]["pageInfo"]
        pull_requests = result["repository"]["pullRequests"]["nodes"]

        savePullRequests(pull_requests)

        saved_pull_requests += PRS_PER_REQUEST
        print(f"Saved {saved_pull_requests} PRs. Spent costs: {current_cost}")

        has_next_page =  page_info["hasNextPage"]
        next_page_after = None if not has_next_page else page_info["endCursor"]
        time.sleep(.1)
    except TransportServerError as e:
        err = str(e)
        if not "rate limit exceeded" in err and not "Unauthorized" in err:
            print(f"Unexpected exception: \"{err}\"\nCheck the log for more details")
            logger.error(repr(e))
            safeExit()
        current_token_idx += 1
        if current_token_idx >= len(token_pool):
            print("Rate limit exceeded or Unauthorized and no more API tokens")
            safeExit()


logger.info("Finished")



