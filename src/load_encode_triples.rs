fn read_file(filename: &str) -> impl Iterator<Item = String> {
    use std::fs::File;
    use std::io::{BufRead, BufReader};
    let file = BufReader::new(File::open(filename).unwrap());
    file.lines().filter_map(|line| line.ok())
}

pub fn load3enc<'a>(
    filename: &str,
) -> impl Iterator<Item = (usize, usize, usize)> + 'a {
   read_file(&format!("{}", filename))
        .map(move |line| {
            let mut elts = line.split(' ');
            (
                elts.next().unwrap().parse().unwrap(),
                elts.next().unwrap().parse().unwrap(),
                elts.next().unwrap().parse().unwrap(),
            )
        })
}
