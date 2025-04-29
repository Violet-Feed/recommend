use tonic_build;

fn main() {
    tonic_build::configure()
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .message_attribute(".", "#[serde(default)]")
        .extern_path(".common", "crate::common")
        .compile_protos(&["src/proto/common.proto","src/proto/recommend.proto","src/proto/im.proto"], &["src"]).unwrap();
}