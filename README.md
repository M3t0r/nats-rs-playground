- [Start NATS](https://docs.nats.io/running-a-nats-service/introduction/running)
```bash
docker run -p 4222:4222 -d --name nats-server nats:latest -js
```
- Create "schema"
```bash
cargo run -- schema --server localhost:4222
```
- Run a worker
```bash
cargo run -- worker --server localhost:4222
```
- Queue a few jobs
```bash
cargo run -- queue --server localhost:4222 1 1
cargo run -- queue --server localhost:4222 2 2
cargo run -- queue --server localhost:4222 2 3
cargo run -- queue --server localhost:4222 3 4
```
- List results
```bash
cargo run -- ls --server localhost:4222
```
- Detailed history
```bash
cargo run -- history --server localhost:4222 2
```

I used [`postcard`](https://www.youtube.com/watch?v=HtBFvTH5ZKE) because I
wanted to try it out. JSON would work just as well...
