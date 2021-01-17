//! Wrapper for a stream that stores the number
//! of bytes read so that uploading to S3 using the `rusoto_s3`
//! crate can indicate upload progress for larger files.
//!
//! See the test for example usage and run with `--nocapture` to 
//! see the mock progress bar:
//!
//! ```ignore
//! cargo test -- --nocapture
//! ```
use bytes::Bytes;
use futures::stream::Stream;
use futures::task::{Context, Poll};
use pin_project_lite::pin_project;
use std::io::Result;
use std::pin::Pin;

/// Progress handler is called with information about the stream read progress.
///
/// The first argument is the amount of bytes that were just read from the 
/// current chunk and the second argument is the total number of bytes read.
pub type ProgressHandler = Box<dyn FnMut(u64, u64) + Send + Sync + 'static>;

pin_project! {
    /// Wrap a stream and store the number of bytes read.
    pub struct ReadProgressStream<T> {
        #[pin]
        inner: Pin<Box<T>>,
        bytes_read: u64,
        progress: ProgressHandler,
        marker: std::marker::PhantomData<T>,
    }
}

impl<T> ReadProgressStream<T>
where
    T: Stream<Item = Result<Bytes>> + Send + Sync + 'static,
{
    /// Create a wrapped stream.
    ///
    /// The progress function will be called as bytes are read from the underlying stream.
    pub fn new(inner: T, progress: ProgressHandler) -> Self {
        ReadProgressStream {
            inner: Box::pin(inner),
            progress,
            bytes_read: 0,
            marker: std::marker::PhantomData {},
        }
    }
}

impl<T> Stream for ReadProgressStream<T>
where
    T: Stream<Item = Result<Bytes>> + Send + Sync + 'static,
{
    type Item = Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        match this.inner.poll_next(cx) {
            Poll::Ready(reader) => match reader {
                Some(result) => match result {
                    Ok(bytes) => {
                        *this.bytes_read += bytes.len() as u64;
                        (this.progress)(bytes.len() as u64, this.bytes_read.clone());
                        Poll::Ready(Some(Ok(bytes)))
                    }
                    Err(e) => Poll::Ready(Some(Err(e))),
                },
                None => Poll::Ready(None),
            },
            Poll::Pending => Poll::Pending,
        }
    }
}

#[test]
fn bytes_progress() -> Result<()> {
    use std::{thread, path::PathBuf, time::Duration};
    use futures::{StreamExt, TryStreamExt};
    use rusoto_core::ByteStream;
    use tokio::fs::File;
    use tokio::runtime::Runtime;
    use tokio_util::codec::{BytesCodec, FramedRead};
    use pbr::{ProgressBar, Units};

    let rt = Runtime::new().unwrap();

    rt.block_on(async {
        let path = PathBuf::from("tests/big-enough-to-buffer.mp4");
        let file = File::open(&path).await?;
        let size = file.metadata().await?.len();
        let reader = FramedRead::new(file, BytesCodec::new())
            .map_ok(|r| r.freeze());

        // Mock progress bar
        let mut pb = ProgressBar::new(size);
        pb.set_units(Units::Bytes);
        pb.show_speed = false;
        if let Some(name) = path.file_name() {
            let msg = format!("{} ", name.to_string_lossy());
            pb.message(&msg);
        }

        // Progress handler to be called as bytes are read
        let progress = Box::new(move |amount: u64, _| {
            pb.add(amount);
            // So we can view the progress
            thread::sleep(Duration::from_millis(5));
        });

        // Wrap the read stream
        let stream = ReadProgressStream::new(reader, progress);

        // Normally this would be passed to a `rusoto` request object
        let body = ByteStream::new_with_size(stream, size as usize);

        // Consume the stream
        let mut content = FramedRead::new(
            body.into_async_read(), BytesCodec::new());

        let mut total_bytes = 0u64;
        while let Some(bytes) = content.next().await {
            total_bytes += bytes?.len() as u64;
        }
        assert_eq!(size, total_bytes);

        Ok::<(), std::io::Error>(())
    })?;

    Ok(())
}
