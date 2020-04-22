use crate::data::SeriesData;
use crate::storage_buffer::StorageBuffer;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::marker::PhantomData;
use std::path::Path;

pub struct Table<P: AsRef<Path>, T: SeriesData, B: StorageBuffer<T, File>> {
    path: P,
    storage_file: File,
    storage_buffer: B,
    index_file: File,
    phantom: PhantomData<T>,
}

impl<P: AsRef<Path>, T: SeriesData, B: StorageBuffer<T, File>> Table<P, T, B> {
    pub fn overwrite(path: P, storage_buffer: B) -> Result<Self, std::io::Error> {
        let storage_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(path.as_ref().with_extension("table"))?;
        let index_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(path.as_ref().with_extension("index"))?;

        Ok(Table {
            path,
            storage_file,
            storage_buffer,
            index_file,
            phantom: Default::default(),
        })
    }

    pub fn insert(&mut self, value: T) -> Result<(), std::io::Error> {
        if self.storage_buffer.insert(value)? {
            self.storage_buffer.flush_into(&mut self.storage_file)?;
        }

        Ok(())
    }

    pub fn close(mut self) -> Result<(), std::io::Error> {
        // Flush any cache
        self.storage_buffer.flush_into(&mut self.storage_file)?;
        self.storage_file.flush()?;
        self.index_file.flush()?;
        Ok(())
    }
}
