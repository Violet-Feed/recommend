use tonic_build;

fn main() {
    tonic_build::configure()
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .compile_protos(&["src/proto/common.proto","src/proto/recommend.proto"], &["src"]).unwrap();
}