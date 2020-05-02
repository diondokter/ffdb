use crate::data::SeriesData;
use crate::storage_buffer::StorageBuffer;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

pub struct Table<P: AsRef<Path>, T: SeriesData, B: StorageBuffer<T, File>> {
    path: P,
    storage_file: File,
    storage_buffer: B,
    index_file: File,
    phantom: PhantomData<T>,
}

impl<P: AsRef<Path>, T: SeriesData, B: StorageBuffer<T, File>> Table<P, T, B> {
    fn get_table_file_path(base_path: &P) -> PathBuf {
        base_path.as_ref().with_extension("table")
    }
    fn get_index_file_path(base_path: &P) -> PathBuf {
        base_path.as_ref().with_extension("index")
    }

    pub fn open(path: P, storage_buffer: B) -> Result<Self, std::io::Error> {
        let storage_file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(Self::get_table_file_path(&path))?;
        let index_file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(Self::get_index_file_path(&path))?;

        Ok(Table {
            path,
            storage_file,
            storage_buffer,
            index_file,
            phantom: Default::default(),
        })
    }

    pub fn delete(self) -> Result<(), std::io::Error> {
        std::fs::remove_file(Self::get_table_file_path(&self.path))?;
        std::fs::remove_file(Self::get_index_file_path(&self.path))?;
        Ok(())
    }

    pub fn close(mut self) -> Result<(), std::io::Error> {
        self.flush()
    }

    pub fn flush(&mut self) -> Result<(), std::io::Error> {
        // Flush any cache
        self.storage_buffer.flush_into(&mut self.storage_file)?;
        self.storage_file.flush()?;
        self.index_file.flush()?;
        Ok(())
    }

    pub fn push(&mut self, value: T) -> Result<(), std::io::Error> {
        if self.storage_buffer.push(value)? {
            self.storage_file.seek(SeekFrom::End(0))?;
            self.storage_buffer.flush_into(&mut self.storage_file)?;
        }

        Ok(())
    }

    /// Search for the first element where the condition is true
    ///
    /// `buffer_size` is in number of items, not in bytes.
    /// The returned usize is the index of the found element.
    pub fn search_first<F>(
        &mut self,
        buffer_size: usize,
        condition: F,
    ) -> Result<Option<usize>, std::io::Error>
    where
        F: FnOnce(T::SeriesType) -> bool + Copy,
    {
        // Do a linear search
        // Todo: Optimize search to something like a binary search

        // Seek the start of the file
        self.storage_file.seek(SeekFrom::Start(0))?;

        // Calculate the buffer size, but now in bytes.
        let buffer_size = buffer_size * T::SIZE;
        // Create the read buffer. Mostly this is for performance.
        let mut read_buffer = vec![0u8; buffer_size];

        // Keep track of where we are in the search
        let mut bytes_read = 0;

        loop {
            // Read (part of) the file into the buffer
            let read_size = self.storage_file.read(&mut read_buffer)?;

            // If there's nothing to read, break.
            if read_size == 0 {
                break;
            }

            // Deserialize the last element. If the condition isn't true for the last one,
            // then it won't be true for the ones before it.
            let last_buffer_element = T::deserialize_from(
                &mut read_buffer.as_mut_slice()[(read_size - T::SIZE)..read_size],
            );
            // Only run the loop if the last one is true.
            if condition(last_buffer_element.get_series_data()) {
                // Loop through the buffer to find the value where the condition flips from false to true.
                for index in (0..read_size).step_by(T::SIZE) {
                    let element = T::deserialize_from(
                        &mut read_buffer.as_mut_slice()[index..(index + T::SIZE)],
                    );
                    if condition(element.get_series_data()) {
                        return Ok(Some(bytes_read / T::SIZE));
                    } else {
                        bytes_read += T::SIZE;
                    }
                }
            }

            // The file didn't have enough for a full buffer, so we're at the end of the file.
            // We can jump out.
            if read_size < buffer_size {
                break;
            }
        }

        // We didn't find anything where the condition is true
        Ok(None)
    }
}
