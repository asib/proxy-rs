use futures::future::join_all;
use serde::Deserialize;
use std::{collections::VecDeque, error::Error, net::SocketAddr, sync::Arc};
use tokio::io::copy_bidirectional;
use tokio::net::{TcpListener, TcpStream};
use tokio::{self, sync::Mutex};

type Result<T, E = Box<dyn Error>> = std::result::Result<T, E>;

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct Port(u16);

fn create_listener_addresses(ports: Vec<Port>) -> Vec<SocketAddr> {
    ports
        .iter()
        .map(|port| SocketAddr::from(([127, 0, 0, 1], port.0)))
        .collect()
}

async fn create_listeners(ports: Vec<Port>) -> Result<Vec<TcpListener>, std::io::Error> {
    let addresses = create_listener_addresses(ports);
    let mut listeners = Vec::with_capacity(addresses.len());

    for address in addresses {
        listeners.push(TcpListener::bind(address).await.unwrap());
    }

    Ok(listeners)
}

struct Route {
    ports: Vec<Port>,
    targets: Arc<Mutex<VecDeque<String>>>,
}

impl Route {
    async fn next_target_stream(&self) -> TcpStream {
        let targets_clone = self.targets.clone();
        let mut targets = targets_clone.lock().await;

        let mut result = None;
        for target in targets.iter() {
            match TcpStream::connect(target).await {
                Ok(stream) => {
                    println!("routing to {}", target);
                    result = Some((target.clone(), stream));
                    break;
                }

                Err(e) => println!("{}: {}", target, e),
            }
        }

        let (target, target_stream) = match result {
            Some(val) => val,
            None => panic!("no available targets"),
        };

        // We need to put all the failed targets into the back of the queue in the correct order.
        loop {
            let _targets = targets.clone();
            let front = _targets.front().unwrap();
            let val = targets.pop_front().unwrap();
            targets.push_back(val);

            if front == &target {
                break;
            }
        }

        target_stream
    }
}

struct Router {
    routes: Vec<Route>,
}

impl Router {
    async fn get_target_stream(&self, source: SocketAddr) -> TcpStream {
        let route = self
            .routes
            .iter()
            .find(|route| route.ports.contains(&Port(source.port())))
            .expect(&format!(
                "failed to find route for source address: {}",
                source
            ));

        route.next_target_stream().await
    }

    async fn handle_connection(&self, mut local_stream: TcpStream) -> Result<()> {
        let mut remote_stream = self
            .get_target_stream(local_stream.local_addr().unwrap())
            .await;

        copy_bidirectional(&mut local_stream, &mut remote_stream).await?;

        Ok(())
    }
}

struct RouteConfig {
    ports: Vec<Port>,
    targets: Vec<String>,
}

struct Proxy {
    router: Arc<Router>,
}

impl Proxy {
    fn new(proxy_routes: Vec<RouteConfig>) -> Proxy {
        Proxy {
            router: Arc::new(Router {
                routes: proxy_routes
                    .into_iter()
                    .map(|app| Route {
                        ports: app.ports.clone(),
                        targets: Arc::new(Mutex::new(app.targets.into())),
                    })
                    .collect(),
            }),
        }
    }

    async fn run(self) -> Result<()> {
        let ports = self
            .router
            .routes
            .iter()
            .flat_map(|route| route.ports.clone())
            .collect();
        let listeners = create_listeners(ports).await?;
        let mut join_handles = Vec::with_capacity(listeners.len());

        for listener in listeners {
            let router = self.router.clone();

            let handle =
                tokio::spawn(async move {
                    loop {
                        match listener.accept().await {
                            Ok((stream, _)) => {
                                let router = router.clone();
                                let local_addr = stream.local_addr();

                                tokio::spawn(async move {
                                    router.clone().handle_connection(stream).await.expect(
                                        &format!(
                                            "error handling connection from: {:?}",
                                            local_addr
                                        ),
                                    );
                                });
                            }

                            Err(e) => println!("failed to accept incoming connection: {:?}", e),
                        }
                    }
                });

            join_handles.push(handle);
        }

        join_all(join_handles).await;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let proxy = Proxy::new(vec![RouteConfig {
        ports: vec![Port(1337)],
        targets: vec!["127.0.0.1:7331".into()],
    }]);

    proxy.run().await.expect("proxy execution failed");

    Ok(())
}
