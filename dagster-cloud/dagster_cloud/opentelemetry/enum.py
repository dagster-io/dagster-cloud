from enum import Enum


class CompressionEnum(Enum):
    GZIP = "gzip"
    DEFLATE = "deflate"


class LoggingExporterEnum(Enum):
    ConsoleLogExporter = "ConsoleLogExporter"
    HttpProtoOTLPExporter = "HttpProtoOTLPExporter"
    GrpcProtoOTLPExporter = "GrpcProtoOTLPExporter"


class LogRecordProcessorEnum(Enum):
    BatchLogRecordProcessor = "BatchLogRecordProcessor"
    SimpleLogRecordProcessor = "SimpleLogRecordProcessor"


class MetricsExporterEnum(Enum):
    ConsoleMetricExporter = "ConsoleMetricExporter"
    HttpProtoOTLPExporter = "HttpProtoOTLPExporter"
    GrpcProtoOTLPExporter = "GrpcProtoOTLPExporter"


class MetricsReaderEnum(Enum):
    PeriodicExportingMetricReader = "PeriodicExportingMetricReader"
    InMemoryMetricReader = "InMemoryMetricReader"


class TracingExporterEnum(Enum):
    HttpProtoOTLPExporter = "HttpProtoOTLPExporter"
    GrpcProtoOTLPExporter = "GrpcProtoOTLPExporter"


class MetricsInstrumentTypesEnum(Enum):
    Counter = "Counter"
    UpDownCounter = "UpDownCounter"
    Histogram = "Histogram"
    Gauge = "Gauge"
    ObservableCounter = "ObservableCounter"
    ObservableUpDownCounter = "ObservableUpDownCounter"
    ObservableGauge = "ObservableGauge"


class AggregationTemporalityEnum(Enum):
    Cumulative = "Cumulative"
    Delta = "Delta"
    Unspecified = "Unspecified"


class ViewAggregationEnum(Enum):
    DefaultAggregation = "DefaultAggregation"
    DropAggregation = "DropAggregation"
    ExplicitBucketHistogramAggregation = "ExplicitBucketHistogramAggregation"
    ExponentialHistogramAggregation = "ExponentialHistogramAggregation"
    LastValueAggregation = "LastValueAggregation"
    SumAggregation = "SumAggregation"
