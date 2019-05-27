// 1,time,INTEGER,YES
// 2,machine ID,INTEGER,YES
// 3,attribute name,STRING_HASH,YES
// 4,attribute value,STRING_HASH_OR_INTEGER,NO
// 5,attribute deleted,BOOLEAN,YES
#[derive(Debug, Deserialize)]
pub struct MachineAttribute {
    pub time: u64,
    pub machine_id: u64,
    pub attribute_name: String,
    pub attribute_value: Option<String>,
    pub attributed_deleted: bool,
}

pub fn for_each_in_file<F>(file: &str, mut f: F) -> std::io::Result<()>
where
    F: FnMut(MachineAttribute) -> std::io::Result<()>,
{
    use std::fs::File;

    let file = File::open(file)?;
    let mut rdr = csv::Reader::from_reader(file);

    for result in rdr.records() {
        let sr = result?;
        let machine_attrib: MachineAttribute = sr.deserialize(None)?;

        f(machine_attrib)?
    }
    Ok(())
}
