use criterion::{
    black_box, criterion_group, criterion_main, AxisScale, BenchmarkId, Criterion,
    PlotConfiguration, Throughput,
};
use ffdb::data::SeriesData;
use ffdb::storage_buffer::HeapBuffer;
use ffdb::table::Table;
use rand::Rng;
use std::convert::TryInto;

pub fn insert_benchmark(c: &mut Criterion) {
    const KB: usize = 1024;
    const MB: usize = 1024 * KB;
    let mut buffered_group = c.benchmark_group("Heap Buffered");
    for size in [
        KB,
        2 * KB,
        4 * KB,
        8 * KB,
        16 * KB,
        64 * KB,
        128 * KB,
        256 * KB,
        512 * KB,
        MB,
        16 * MB,
    ]
    .iter()
    {
        buffered_group.throughput(Throughput::Bytes(TestData::SIZE as u64));

        let mut table = Table::open("target/temp/bench_table", HeapBuffer::new(*size)).unwrap();
        let data = TestData {
            index: 561324,
            data: 461.546,
        };

        buffered_group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.iter(|| table.push(black_box(data)).unwrap())
        });

        table.delete().unwrap();
    }
}

pub fn search_benchmark(c: &mut Criterion) {
    let mut table_group = c.benchmark_group("Table size, 10k element buffer");
    table_group.throughput(Throughput::Elements(1));
    table_group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));

    for size in [
        1_000,
        10_000,
        100_000,
        1_000_000,
        10_000_000,
        100_000_000,
        1_000_000_000,
    ]
    .iter()
    {
        // Create the table
        let mut table = Table::open("target/temp/bench_table", HeapBuffer::new(1024)).unwrap();

        // Fill the table
        for i in 0..*size {
            table
                .push(TestData {
                    index: i,
                    data: rand::random(),
                })
                .unwrap();
        }

        table.flush().unwrap();

        // Do the benchmark
        table_group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.iter(|| {
                let condition_value = rand::thread_rng().gen_range(0, size - 1);
                table
                    .search_first(10000, |data_index| data_index > condition_value)
                    .unwrap()
                    .unwrap();
            });
        });

        // Get rid of the table
        table.delete().unwrap();
    }
}

criterion_group!(benches, insert_benchmark, search_benchmark);
criterion_main!(benches);

#[derive(Copy, Clone, Debug)]
struct TestData {
    index: u64,
    data: f32,
}

impl SeriesData for TestData {
    const SIZE: usize = 12;
    type SeriesType = u64;

    fn get_series_data(&self) -> Self::SeriesType {
        self.index
    }

    fn serialize_into<T: std::io::Write>(&self, target: &mut T) -> Result<(), std::io::Error> {
        let mut buffer = [0; Self::SIZE];
        buffer[0..8].copy_from_slice(&self.index.to_le_bytes());
        buffer[8..12].copy_from_slice(&self.data.to_le_bytes());

        target.write_all(&buffer)?;
        Ok(())
    }

    fn deserialize_from(source: &[u8]) -> Self {
        Self {
            index: u64::from_le_bytes(source[0..8].try_into().unwrap()),
            data: f32::from_le_bytes(source[8..12].try_into().unwrap()),
        }
    }
}
