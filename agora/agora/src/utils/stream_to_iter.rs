use futures_util::stream::{Stream, StreamExt};
use std::pin::Pin;
use tokio::runtime::Handle;

/// A wrapper that turns a Stream into a blocking Iterator.
/// Optimized for subscriber streams which return `Pin<Box<dyn Stream<Item = T> + Send>>`.
pub struct BlockingStreamIterator<T> {
    stream: Pin<Box<dyn Stream<Item = T> + Send>>,
    rt_handle: Handle,
}

impl<T> BlockingStreamIterator<T> {
    /// Creates a new blocking stream iterator wrapper.
    /// Requires a Tokio runtime handle to execute the async stream operations.
    pub fn new(stream: Pin<Box<dyn Stream<Item = T> + Send>>, rt_handle: Handle) -> Self {
        Self { stream, rt_handle }
    }
}

impl<T> Iterator for BlockingStreamIterator<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        // Use block_in_place to allow blocking on async operations within an async context
        tokio::task::block_in_place(|| self.rt_handle.block_on(self.stream.next()))
    }
}

/// Convenience function to convert subscriber streams to blocking iterators
pub fn stream_to_iter<T>(
    stream: Pin<Box<dyn Stream<Item = T> + Send>>,
    rt_handle: Handle,
) -> BlockingStreamIterator<T> {
    BlockingStreamIterator::new(stream, rt_handle)
}
