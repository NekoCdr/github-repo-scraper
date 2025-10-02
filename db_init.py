from pathlib import Path
import sqlite3

db_path = Path("github_statistics.db")

con = sqlite3.connect(db_path)
con.execute("PRAGMA foreign_keys = ON")

con.execute("""
    create table if not exists authors(
            id integer primary key,
            login text unique not null,
            name text
        )
""")

con.execute("""
    create table if not exists pull_requests(
            id integer primary key,
            author_id integer,
            title text not null,
            state text not null,
            created_at text not null,
            closed_at text,
            merged_at text,
            additions int not null,
            deletions int not null,
            author_association text not null,
            head_repository_url text,
            is_cross_repository integer default 0 not null,
            merge_commit_ci_state text,
            api_total_comments_count integer not null,

            foreign key (author_id) references authors(id) on delete set null
        )
""")

con.execute("""
    create table if not exists commits(
            id integer primary key,
            pull_request_id integer,
            url text,
            committed_at text not null,
            unique(pull_request_id, url),

            foreign key (pull_request_id) references pull_requests(id) on delete set null
        )
""")

con.execute("""
    create table if not exists reviews(
            id integer primary key,
            author_id integer,
            pull_request_id integer,
            created_at text not null,

            foreign key (author_id) references authors(id) on delete set null,
            foreign key (pull_request_id) references pull_requests(id) on delete set null
        )
""")

con.execute("""
    create table if not exists review_threads(
            id integer primary key,
            node_id text unique not null,
            pull_request_id integer,

            foreign key (pull_request_id) references pull_requests(id) on delete set null
        )
""")

con.execute("""
    create table if not exists comments(
            id integer primary key,
            author_id integer,
            pull_request_id integer,
            review_id integer,
            review_thread_id integer,
            created_at text not null,

            foreign key (author_id) references authors(id) on delete set null,
            foreign key (pull_request_id) references pull_requests(id) on delete set null,
            foreign key (review_id) references reviews(id) on delete set null,
            foreign key (review_thread_id) references review_threads(id) on delete set null
        )
""")

con.execute("""
    create table if not exists files(
            id integer primary key,
            pull_request_id integer,
            path text not null,
            change_type integer,
            additions integer,
            deletions integer,
            unique(pull_request_id, path),

            foreign key (pull_request_id) references pull_requests(id) on delete cascade
        )
""")

con.execute("""
    create table if not exists author_pull_request(
            author_id integer,
            pull_request_id integer,
            unique(author_id, pull_request_id),

            foreign key (author_id) references authors(id) on delete cascade,
            foreign key (pull_request_id) references pull_requests(id) on delete cascade
        )
""")

con.execute("""
    create table if not exists labels(
            id integer primary key,
            name text unique not null
        )
""")

con.execute("""
    create table if not exists label_pull_request(
            label_id integer,
            pull_request_id integer,
            unique(label_id, pull_request_id),

            foreign key (label_id) references labels(id) on delete cascade,
            foreign key (pull_request_id) references pull_requests(id) on delete cascade
        )
""")

con.close()