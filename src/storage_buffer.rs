use crate::data::SeriesData;
use std::io::Write;

pub trait StorageBuffer<T: SeriesData, W: Write> {
    /// Push the data to the buffer.
    /// If it must be flushed, then true is returned.
    fn push(&mut self, data: T) -> Result<bool, std::io::Error>;
    /// Empties the buffer into the given write object.
    fn flush_into(&mut self, target: &mut W) -> Result<(), std::io::Error>;
}

pub struct Unbuffered<T: SeriesData> {
    buffer: Option<T>,
}

impl<T: SeriesData> Unbuffered<T> {
    pub fn new() -> Self {
        Self { buffer: None }
    }
}

impl<T: SeriesData, W: Write> StorageBuffer<T, W> for Unbuffered<T> {
    fn push(&mut self, data: T) -> Result<bool, std::io::Error> {
        self.buffer = Some(data);
        Ok(true)
    }

    fn flush_into(&mut self, target: &mut W) -> Result<(), std::io::Error> {
        if let Some(data) = self.buffer.take() {
            data.serialize_into(target)?;
        }

        Ok(())
    }
}

pub struct HeapBuffer {
    buffer: Vec<u8>,
}

impl HeapBuffer {
    pub fn new(buffer_size: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(buffer_size),
        }
    }
}

impl<T: SeriesData, W: Write> StorageBuffer<T, W> for HeapBuffer {
    fn push(&mut self, data: T) -> Result<bool, std::io::Error> {
        // Extend the buffer.
        // This is safe because the length is never bigger than the capacity
        // and the new data is initialized.
        unsafe {
            self.buffer
                .set_len((self.buffer.len() + T::SIZE).min(self.buffer.capacity()));
            let range = (self.buffer.len() - T::SIZE)..self.buffer.len();
            data.serialize_into::<&mut [u8]>(&mut &mut self.buffer[range])?;
        };

        Ok(self.buffer.len() + T::SIZE > self.buffer.capacity())
    }

    fn flush_into(&mut self, target: &mut W) -> Result<(), std::io::Error> {
        target.write_all(self.buffer.as_slice())?;
        self.buffer.clear();

        Ok(())
    }
}
