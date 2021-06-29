fn read_file(filename: &str) -> impl Iterator<Item = String> {
    use std::fs::File;
    use std::io::{BufRead, BufReader};
    let file = BufReader::new(File::open(filename).unwrap());
    file.lines().filter_map(|line| line.ok())
}

pub fn load3<'a>(
    index: usize,
    prefix: &str,
    filename: &str,
) -> impl Iterator<Item = (u32, u32, u32)> + 'a {
    println!("{:?}", prefix);

    read_file(&format!("{}{}", prefix, filename))
        .filter(move |_| index == 0)
        .map(move |line| {
            let mut line_clean = line.replace("(", "");
            line_clean = line_clean.replace(")", "");
            let mut elts = line_clean.split(", ");
            (
                elts.next().unwrap().parse().unwrap(),
                elts.next().unwrap().parse().unwrap(),
                elts.next().unwrap().parse().unwrap(),
            )
        })
}
