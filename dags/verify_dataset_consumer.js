import { dag } from 'airflow-nodejs/dag/types';
// §10 dataset consumer
export default dag({
  id: 'verify_dataset_consumer',
  schedule: null,
  datasets: ['test://verify/output'],
  tasks: {
    consume: { run: async () => 'triggered by dataset event' }
  }
})
