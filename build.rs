use tonic_build;

fn main() {
    tonic_build::configure()
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .message_attribute(".", "#[serde(default)]")
        .compile_protos(
            &["src/proto/common.proto", "src/proto/recommend.proto"],
            &["src"],
        )
        .unwrap();
}
