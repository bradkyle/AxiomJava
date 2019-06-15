package(default_visibility = ["//visibility:public"])

licenses(["notice"])  # Apache 2.0


JAVACOPTS = [
    "-Xlint:unchecked",
    "-source",
    "1.8",
]

java_library(
    name = "util",
    javacopts = JAVACOPTS,
    srcs = glob(["**/*.java"]),
    deps = [
        "//third_party:com_fasterxml_jackson_core_jackson_core",
        "//third_party:com_fasterxml_jackson_core_jackson_databind",
        "//third_party:com_fasterxml_jackson_core_jackson_annotations",
        "//third_party:com_google_guava_guava",
        "//third_party:joda_time_joda_time",
        "//third_party:org_apache_avro_avro",
        "//third_party:org_apache_beam_beam_sdks_java_core",
        "//third_party:org_apache_beam_beam_sdks_java_io_google_cloud_platform",
        "//third_party:org_slf4j_slf4j_api",
        "//third_party:com_google_code_gson_gson"
    ],
)
