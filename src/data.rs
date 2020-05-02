pub trait SeriesData {
    const SIZE: usize;
    type SeriesType: Ord + Eq;

    fn get_series_data(&self) -> Self::SeriesType;

    fn serialize_into<T: std::io::Write>(&self, target: &mut T) -> Result<(), std::io::Error>;
    fn deserialize_from(source: &[u8]) -> Self
    where
        Self: Sized;
}
