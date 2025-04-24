use tonic_build;

fn main() {
    tonic_build::compile_protos("src/proto/common.proto").unwrap();
    tonic_build::compile_protos("src/proto/recommend.proto").unwrap();
}    