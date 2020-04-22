use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput, PlotConfiguration};
use ffdb::data::SeriesData;
use ffdb::storage_buffer::{HeapBuffer, Unbuffered};
use ffdb::table::Table;

pub fn insert_benchmark(c: &mut Criterion) {
    const KB: usize = 1024;
    const MB: usize = 1024 * KB;
    let mut buffered_group = c.benchmark_group("Heap Buffered");
    for size in [KB, 2 * KB, 4 * KB, 8 * KB, 16 * KB, 64 * KB, 128 * KB, 256 * KB, 512 * KB, MB, 16 * MB].iter() {
        buffered_group.throughput(Throughput::Bytes(TestData::SIZE as u64));

        let mut table = Table::overwrite("target/temp/bench_table", HeapBuffer::new(*size)).unwrap();
        let data = TestData {
            index: 561324,
            data: 461.546,
        };

        buffered_group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.iter(|| table.insert(black_box(data)).unwrap())
        });
    }
}

criterion_group!(benches, insert_benchmark);
criterion_main!(benches);

#[derive(Copy, Clone, Debug)]
struct TestData {
    index: u64,
    data: f32,
}

impl SeriesData for TestData {
    const SIZE: usize = 12;
    type Index = u64;

    fn get_index(&self) -> Self::Index {
        self.index
    }

    fn serialize_into<T: std::io::Write>(&self, target: &mut T) -> Result<(), std::io::Error> {
        let mut buffer = [0; Self::SIZE];
        buffer[0..8].copy_from_slice(&self.index.to_le_bytes());
        buffer[8..12].copy_from_slice(&self.data.to_le_bytes());

        target.write_all(&buffer)?;
        Ok(())
    }

    fn deserialize_from<T: std::io::Read>(source: &mut T) -> Result<Self, std::io::Error> {
        let mut index_buffer = [0; 8];
        source.read_exact(&mut index_buffer).unwrap();
        let mut data_buffer = [0; 4];
        source.read_exact(&mut data_buffer).unwrap();

        Ok(Self {
            index: u64::from_le_bytes(index_buffer),
            data: f32::from_le_bytes(data_buffer),
        })
    }
}
