use csv;
use serde::de::DeserializeOwned;
use std::fs::File;
use std::iter::Iterator;
use std::marker::PhantomData;

pub(crate) struct TraceFileIterator<T> {
    path: String,
    file_num: usize,
    num_files: usize,
    reader: csv::Reader<File>,
    phantom: PhantomData<T>,
}

impl<T> TraceFileIterator<T>
where
    T: DeserializeOwned,
{
    pub fn new(f: &str, num_files: usize) -> Self {
        TraceFileIterator {
            path: f.to_owned(),
            file_num: 0,
            num_files: num_files,
            reader: csv::Reader::from_path(Self::filename(f, 0, num_files)).unwrap(),
            phantom: PhantomData,
        }
    }

    fn filename(path: &str, i: usize, num: usize) -> String {
        format!("{}/part-{:05}-of-{:05}.csv", path, i, num)
    }

    fn next_file(&mut self) -> Option<()> {
        if self.file_num >= self.num_files {
            None
        } else {
            self.file_num += 1;
            self.reader =
                csv::Reader::from_path(Self::filename(&self.path, self.file_num, self.num_files))
                    .unwrap();
            Some(())
        }
    }
}

impl<T> Iterator for TraceFileIterator<T>
where
    T: DeserializeOwned,
{
    type Item = Result<T, csv::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut sr = csv::StringRecord::new();
        if !self.reader.read_record(&mut sr).unwrap() {
            self.next_file().and(self.next())
        } else {
            Some(sr.deserialize(None))
        }
    }
}
