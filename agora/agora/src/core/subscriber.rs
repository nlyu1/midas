use super::common::Agorable;
use crate::metaserver::AgoraClient;
use crate::ping::PingClient;
use crate::rawstream::RawStreamClient;
use crate::utils::OrError;
use std::marker::PhantomData;

pub struct Subscriber<T: Agorable> {
    metaclient: AgoraClient,
    rawstreamclient: RawStreamClient<Vec<u8>>,
    pingclient: PingClient,
    _phantom: PhantomData<T>,
}

impl<T: Agorable> Subscriber<T> {
    pub fn new(path: String) -> OrError<Self<T>> {}
}

pub struct OmniSubscriber {
    metaclient: AgoraClient,
    rawstreamclient: RawStreamClient<String>,
}
