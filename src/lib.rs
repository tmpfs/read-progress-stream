//! Wrapper stream for `tokio::fs::File` that stores the number 
//! of bytes read so that uploading to S3 using the `rusoto_s3` 
//! crate can indicate upload progress for larger files.
//!
use std::pin::Pin;
use pin_project_lite::pin_project;
use futures::task::{Context, Poll};
use futures::stream::Stream;
use futures_util::TryStreamExt;
use tokio_util::codec;
use bytes::Bytes;

type ProgressHandler = Box<dyn Fn(u64, u64) + Send + Sync + 'static>;

pin_project! {
    pub struct ReadProgressStream {
        #[pin]
        inner: Pin<Box<dyn Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static>>,
        size: u64,
        bytes_read: u64,
        progress: ProgressHandler,
    }
}

impl ReadProgressStream {
    pub fn new(file: tokio::fs::File, size: u64, progress: ProgressHandler) -> Self {
        let reader = codec::FramedRead::new(file, codec::BytesCodec::new());
        let inner = Box::pin(reader.map_ok(|r| r.freeze()));
        ReadProgressStream { inner, size, progress, bytes_read: 0 }
    }
}

impl Stream for ReadProgressStream {
    type Item = std::io::Result<Bytes>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>
    ) -> Poll<Option<Self::Item>> {
        let this = self.project();
        match this.inner.poll_next(cx) {
            Poll::Ready(reader) => {
                match reader {
                    Some(result) => {
                        match result {
                            Ok(bytes) => {
                                *this.bytes_read += bytes.len() as u64;
                                (this.progress)(this.bytes_read.clone(), this.size.clone());
                                Poll::Ready(Some(Ok(bytes)))
                            }
                            Err(e) => Poll::Ready(Some(Err(e)))
                        }
                    }
                    None => Poll::Ready(None)
                }
            } 
            Poll::Pending => Poll::Pending,
        }
    }
}
