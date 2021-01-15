//! Wrapper stream for `tokio::fs::File` that stores the number 
//! of bytes read so that uploading to S3 using the `rusoto_s3` 
//! crate can indicate upload progress for larger files.
//!
//! Be aware that currently this uses an old version of tokio (`0.22.2`) 
//! as we have a project that has many dependencies and are deferring an 
//! upgrade to `tokio@1.0` until it has propagated thoroughly within the 
//! crates ecosystem. As soon as we can update the project using this crate 
//! we will update to `tokio@1.0`.
//!
//! ```ignore
//! // Prepare the file for upload
//! let file = std::fs::File::open(&path)?;
//! let size = file.metadata()?.len();
//! let file = tokio::fs::File::from_std(file);
//!
//! // Progress handler to be called as bytes are read
//! let progress = Box::new(|read: u64, total: u64| {
//!     println!("Uploaded {} / {}", read, total);
//! });
//! let stream = ReadProgressStream::new(file, size, progress);
//!
//! // Create a `ByteStream` from `rusoto_core`
//! let body = ByteStream::new_with_size(stream, size as usize);
//!
//! // Now assign the to the `body` of a `PutObjectRequest` and 
//! // call `put_object()` on an `S3Client`.
//! ```
//!
use std::pin::Pin;
use pin_project_lite::pin_project;
use futures::task::{Context, Poll};
use futures::stream::Stream;
use futures_util::TryStreamExt;
use tokio_util::codec;
use bytes::Bytes;

type ProgressHandler = Box<dyn FnMut(u64, u64) + Send + Sync + 'static>;

pin_project! {
    /// Wrap a tokio File and store the bytes read.
    pub struct ReadProgressStream {
        #[pin]
        inner: Pin<Box<dyn Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static>>,
        size: u64,
        bytes_read: u64,
        progress: ProgressHandler,
    }
}

impl ReadProgressStream {

    /// Create a wrapped stream for the File.
    ///
    /// The progress function will be called as bytes are read from the underlying file.
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
