use std::time::Duration;

use futures::StreamExt;
use serde::{Serialize, Deserialize};
use clap::{Parser, Subcommand, Args};
use async_nats::{ConnectError, Client};

const WORKER_QUEUE_STREAM: &str = "jobs";
const WORKER_QUEUE_CONSUMER: &str = "workers";
const WORKER_RESULTS_STORE: &str = "results";

/// NATS test cli
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Worker {
        #[command(flatten)]
        nats_connection: NATSConnection,
    },
    Queue {
        #[command(flatten)]
        nats_connection: NATSConnection,

        number: i64,
        factor: u64,
    },
    Ls {
        #[command(flatten)]
        nats_connection: NATSConnection,
    },
    History {
        #[command(flatten)]
        nats_connection: NATSConnection,

        number: i64,
    },
    Schema {
        #[command(flatten)]
        nats_connection: NATSConnection,
    },
}

#[derive(Debug, Args)]
struct NATSConnection {
    #[arg(short, long)]
    server: String,
}

impl NATSConnection {
    pub async fn connect(self) -> Result<Client, ConnectError> {
        async_nats::connect(self.server).await
    }
}

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    let args = Cli::parse();

    match args.command {
        Commands::Worker { nats_connection } => {
            let client = nats_connection.connect().await?;
            let js = async_nats::jetstream::new(client);

            let stream = js.get_or_create_stream(WORKER_QUEUE_STREAM).await?;
            let consumer: async_nats::jetstream::consumer::PullConsumer = stream.get_consumer(WORKER_QUEUE_CONSUMER).await?;
            let mut jobs = consumer.messages().await?;

            let result_store = js.get_key_value(WORKER_RESULTS_STORE).await?;

            while let Some(job_msg) = jobs.next().await {
                let job_msg = job_msg?;
                let job: WorkerJob = postcard::from_bytes(&job_msg.payload)?;
                dbg!(&job);
                match job {
                    WorkerJob::V1(..) => job_msg.ack_with(async_nats::jetstream::AckKind::Term).await?,
                    WorkerJob::V2(WorkerJobV2 { number, factor }) => {
                        let result = Computation {
                            factorial: (1..=number.unsigned_abs()).product(),
                            factor,
                            product: number * factor as i64,
                            bits: number.ilog2(),
                        };
                        match result_store.put(format!("{number}"), postcard::to_stdvec(&result)?.into()).await {
                            Ok(..) => job_msg.ack().await?,
                            Err(e) => {
                                eprintln!("Error while storing results for {number}: {:?}", e);
                                job_msg.ack_with(async_nats::jetstream::AckKind::Nak(None)).await?;
                            },
                        };
                    },
                }
            }
        },
        Commands::Queue { nats_connection, number, factor } => {
            let client = nats_connection.connect().await?;
            let js = async_nats::jetstream::new(client);

            let job = postcard::to_stdvec(&WorkerJob::V2(WorkerJobV2 { number, factor }))?;
            let resp = js.publish(WORKER_QUEUE_STREAM, job.into()).await?;
            dbg!(resp);
            eprintln!("queued job: {number}");
        },
        Commands::Ls { nats_connection } => {
            let client = nats_connection.connect().await?;
            let js = async_nats::jetstream::new(client);
            let result_store = js.get_key_value(WORKER_RESULTS_STORE).await?;
            let mut keys_iter = result_store.keys().await?;

            // todo: this is super sequential. better: collect into vec of keys, turn into vec of
            // futures, await all futures simultaneously, voila
            while let Some(key) = keys_iter.next().await {
                let key = key?;
                match result_store.get(&key).await {
                    Ok(value) => println!("{key}: {:?}", postcard::from_bytes::<Computation>(&value.unwrap())?),
                    Err(e) => eprintln!("{key}: error: {:?}", e),
                }
            }
        },
        Commands::History { nats_connection, number } => {
            let client = nats_connection.connect().await?;
            let js = async_nats::jetstream::new(client);
            let result_store = js.get_key_value(WORKER_RESULTS_STORE).await?;
            let mut history = result_store.history(format!("{number}")).await?;

            // todo: same as in `ls`
            while let Some(rev) = history.next().await {
                let rev = rev?;
                dbg!(rev);
            }
        },
        Commands::Schema { nats_connection } => {
            let client = nats_connection.connect().await?;
            let js = async_nats::jetstream::new(client);
            let mut has_errors = false;

            let streams = vec![async_nats::jetstream::stream::Config {
                name: WORKER_QUEUE_STREAM.to_owned(),
                discard: async_nats::jetstream::stream::DiscardPolicy::New,
                retention: async_nats::jetstream::stream::RetentionPolicy::WorkQueue,
                ..Default::default()
            }];
            for config in streams {
                match js.create_stream(config.clone()).await {
                    Ok(..) => println!("stream '{}': ok", config.name),
                    Err(e) => {
                        has_errors = true;
                        eprintln!("stream '{}': error: {}", config.name, e)
                    },
                }
            }

            let consumers = vec![
                (
                    WORKER_QUEUE_STREAM,
                    async_nats::jetstream::consumer::Config {
                        durable_name: Some(WORKER_QUEUE_CONSUMER.to_owned()),
                        ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
                        ack_wait: Duration::from_secs(60*60*2),
                        ..Default::default()
                    }
                ),
            ];
            for (stream, config) in consumers {
                let name = match config.durable_name.clone() { Some(n) => format!("{stream}/{n}"), None => stream.to_owned() };
                match js.create_consumer_strict_on_stream(config, stream).await {
                    Ok(..) => println!("consumer '{}': ok", name),
                    Err(e) => {
                        has_errors = true;
                        eprintln!("consumer '{}': error: {}", name, e)
                    },
                }
            }

            let stores = vec![async_nats::jetstream::kv::Config {
                bucket: WORKER_RESULTS_STORE.to_owned(),
                history: 64,
                ..Default::default()
            }];
            for config in stores {
                let name = config.bucket.clone();
                match js.create_key_value(config).await {
                    Ok(..) => println!("kv '{}': ok", name),
                    Err(e) => {
                        has_errors = true;
                        eprintln!("kv '{}': error: {}", name, e)
                    },
                }
            }

            if has_errors { Err("Some NATS resources are outdated")? }
        },
    }

    Ok(())
}

#[derive(Serialize, Deserialize, Debug)]
enum WorkerJob {
    V1(WorkerJobV1),
    V2(WorkerJobV2),
}

#[derive(Serialize, Deserialize, Debug)]
struct WorkerJobV2 {
    number: i64,
    factor: u64,
}

#[derive(Serialize, Deserialize, Debug)]
struct WorkerJobV1 {
    number: i64,
}

#[derive(Serialize, Deserialize, Debug)]
struct Computation {
    factorial: u64,
    factor: u64,
    product: i64,
    bits: u32,
}
