


public class BatchedStatefulAggregation extends PTransform {
    
    public BatchedStatefulAggregation(){

    }
}

public class SideCombineFn extends CombineFn<Row, Map<Double, Double>, Map<Double, Double>> {

    final int bufferSize;

    public SideCombineFn(
        int bufferSize
    ) {
        this.bufferSize=bufferSize;
    }

    @Override
    public Map<Double, Double> createAccumulator() {
        return new HashMap(); 
    }

    @Override
    public Map<Double, Double> addInput(Map<Double, Double> accumulator, Row input) {
        if (Double.compare(input.getDouble("quantity"), 0.000000001) < 0) {
            accumulator.remove(input.getDouble("price"));
        } else {
            accumulator.put(input.getDouble("price"), input.getDouble("quantity"));
        }
        return accumulator;
    }

    @Override
    public Map<Double, Double> mergeAccumulators(Iterable<Map<Double, Double>> accumulators) {
        Map<Double, Double> result = createAccumulator();
        for (Map<Double, Double> value : accumulators) {
        result.putAll(value);
        }
        return result;
    }

    @Override
    public Map<Double, Double> extractOutput(Map<Double, Double> accumulator) {
        return accumulator;
    }
}

public class BatchedStatefulAggregationDoFn extends DoFn<> {

    final int bufferSize;
    final []String features;

    public StatefulAggregationDoFn(
        int bufferSize
    ) {
        this.bufferSize=bufferSize;
    }

    @StateId("buffer")
    private final StateSpec<CombiningState<Row, Map<Long, Row>, Map<Long, Row>>> bufferStateSpec = StateSpecs.combining(
        MapCoder.of(VarIntCoder.of(), DoubleCoder.of()),
        new BufferCombineFn(
            bufferSize
        )
    );


    @ProcessElement
    public void process(
        ProcessContext context,
        BoundedWindow window,
        @StateId("buffer") CombiningState<Row, Map<Long, Row>, Map<Long, Row>> bufferState
    ) {
       bufferState.add();
       if (bufferState.full()){
            Map<Long, Row> = bufferState.read();
            ArrayList[][] table = new ArrayList[bufferSize][features];
            // for row in df convert to array
            for () {
                
            }
            context.output(table);
       }

    }

}