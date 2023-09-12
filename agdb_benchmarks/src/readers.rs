use crate::bench_result::BenchResult;
use crate::config::Config;
use crate::database::Database;
use crate::utilities;
use crate::utilities::measured;
use agdb::DbId;
use agdb::QueryBuilder;
use std::time::Duration;
use tokio::task::JoinHandle;

struct Reader {
    db: Database,
    pub(crate) times: Vec<Duration>,
}

pub(crate) struct Readers(Vec<JoinHandle<Reader>>);

impl Reader {
    pub(crate) fn new(db: Database) -> Self {
        Self { db, times: vec![] }
    }

    fn read_comments(&mut self, limit: u64) -> BenchResult<bool> {
        if let Some(post_id) = self.last_post()? {
            let duration = measured(|| {
                let _comments = self.db.0.read()?.exec(
                    &QueryBuilder::select()
                        .ids(
                            QueryBuilder::search()
                                .from(post_id)
                                .limit(limit)
                                .where_()
                                .distance(agdb::CountComparison::Equal(2))
                                .query(),
                        )
                        .query(),
                )?;
                Ok(())
            })?;

            self.times.push(duration);

            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn read_posts(&mut self, limit: u64) -> BenchResult<bool> {
        let mut result = false;

        let duration = measured(|| {
            let posts = self.db.0.read()?.exec(
                &QueryBuilder::select()
                    .ids(
                        QueryBuilder::search()
                            .from("posts")
                            .limit(limit)
                            .where_()
                            .distance(agdb::CountComparison::Equal(2))
                            .query(),
                    )
                    .query(),
            )?;

            result = posts.result != 0;

            Ok(())
        })?;

        if result {
            self.times.push(duration);
        }

        Ok(result)
    }

    fn last_post(&mut self) -> BenchResult<Option<DbId>> {
        if let Some(post) = self
            .db
            .0
            .read()?
            .exec(
                &QueryBuilder::search()
                    .depth_first()
                    .from("posts")
                    .limit(1)
                    .where_()
                    .distance(agdb::CountComparison::Equal(2))
                    .query(),
            )?
            .elements
            .get(0)
        {
            Ok(Some(post.id))
        } else {
            Ok(None)
        }
    }
}

impl Readers {
    pub(crate) async fn join_and_report(
        &mut self,
        description: &str,
        threads: u64,
        per_thread: u64,
        per_action: u64,
        config: &Config,
    ) -> BenchResult<()> {
        let mut readers = vec![];

        for task in self.0.iter_mut() {
            readers.push(task.await?);
        }

        let times: Vec<Duration> = readers.into_iter().flat_map(|w| w.times).collect();

        utilities::report(description, threads, per_thread, per_action, times, config);

        Ok(())
    }
}

pub(crate) fn start_post_readers(db: &mut Database, config: &Config) -> BenchResult<Readers> {
    let mut tasks = vec![];

    for i in 0..config.post_readers.count {
        let db = db.clone();
        let limit = config.post_readers.posts;
        let read_delay = Duration::from_millis(config.post_readers.delay_ms % (i + 1));
        let reads = config.post_readers.reads_per_reader;

        let handle = tokio::spawn(async move {
            let mut reader = Reader::new(db);
            let mut read = 0;

            while read != reads {
                tokio::time::sleep(read_delay).await;

                if reader.read_posts(limit).unwrap_or(false) {
                    read += 1;
                }
            }

            reader
        });

        tasks.push(handle);
    }

    Ok(Readers(tasks))
}

pub(crate) fn start_comment_readers(db: &mut Database, config: &Config) -> BenchResult<Readers> {
    let mut tasks = vec![];

    for i in 0..config.comment_readers.count {
        let db = db.clone();
        let read_delay = Duration::from_millis(config.comment_readers.delay_ms % (i + 1));
        let reads = config.comment_readers.reads_per_reader;
        let limit = config.comment_readers.comments;

        let handle = tokio::spawn(async move {
            let mut reader = Reader::new(db);
            let mut read = 0;

            while read != reads {
                tokio::time::sleep(read_delay).await;

                if reader.read_comments(limit).unwrap_or(false) {
                    read += 1;
                }
            }

            reader
        });

        tasks.push(handle);
    }

    Ok(Readers(tasks))
}