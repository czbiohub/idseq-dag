import json
from idseq_dag.engine.pipeline_step import PipelineStep
import idseq_dag.util.command as command
import idseq_dag.util.count as count

class PipelineStepCombineTaxonCounts(PipelineStep):
    '''
    Combine counts from gsnap and rapsearch
    '''
    def run(self):
        input_files = []
        for target in self.input_files_local:
            input_files.append(target[3])
        output_file = self.output_files_local()[0]
        PipelineStepRunCombineTaxonCounts.combine_counts(input_files, output_file)


    def count_reads(self):
        pass
    @staticmethod
    def combine_counts(input_json_files, output_json_path):
        taxon_counts = []
        for input_file_name in input_json_files:
            with open(input_file_name, 'r') as input_file:
                data = json.load(input_file)
                taxon_counts += data["pipeline_output"]["taxon_counts_attributes"]
        output_dict = {"pipeline_output": { "taxon_counts_attributes": taxon_counts}}
        with open(output_json_path, 'w') as output_file:
            json.dump(output_dict, output_file)

