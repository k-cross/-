fn main() {
    #[cfg(feature = "grpc")]
    {
        println!("cargo:rerun-if-changed=proto/admission.proto");
        tonic_build::compile_protos("proto/admission.proto").expect("compile admission.proto");
    }
}
