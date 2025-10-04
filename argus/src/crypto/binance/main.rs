pub enum BinanceStreamUpdate {
    Bbo(BboUpdate), 
    Trade,
    OrderbookDiff
}; 

impl BinanceStreamUpdate {
    pub fn of_json(s: String) -> OrError<Self> {
        // Todo 
    }

    pub fn binance_suffix(&self) -> String {
        match self {
            Bbo(_) => "@bbo", 
            OrderbookDiff(_) => "@depth@100ms",
            Trade(_) => "@lastTrade"
        }
    }
}



pub struct BinanceDataWorker<T: BinanceStreamUpdate> {
    agora_paths: Vec<String>, 
    dispatch_handle: JoinHandle<()>,
    _phantom: PhantomData
}

impl<T: BinanceStreamUpdate> BinanceDataWorker {
    pub fn new(stream_tasks: Vec<>, metaserver_addr: Ipv6Addr, metaserver_port: u16, prefix: String) -> OrError<Self> {
        if stream_tasks.len > 1024 {
            return Err("BinanceDataWorker error: don't pass in more than 1024 tasks per worker"); 
        }

        let random_hash = ; // Generate random hash with 6 characters 

        let agora_publishers = stream_tasks.iter().map(
            |(symbol, update_type)| {
                let path = prefix.clone() + T::binance_suffix()
                agora::Publisher::<T>(

                )
            }
        )
        let worker_task = async move {

        }
    }
}

impl<T: BinanceStreamUpdate> Drop for BinanceDataWorker {
    pub fn drop(&mut self) {
        // Upon agora publisher drop, pingserver and rawstream server aborts background handles
        self.dispatch_handle.abort()
    }
}