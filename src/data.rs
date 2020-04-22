pub trait SeriesData {
    const SIZE: usize;
    type Index: Ord + Eq;

    fn get_index(&self) -> Self::Index;

    fn serialize_into<T: std::io::Write>(&self, target: &mut T) -> Result<(), std::io::Error>;
    fn deserialize_from<T: std::io::Read>(source: &mut T) -> Result<Self, std::io::Error>
    where
        Self: Sized;
}
