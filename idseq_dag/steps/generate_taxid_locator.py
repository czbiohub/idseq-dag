import os

from idseq_dag.engine.pipeline_step import PipelineStep
import idseq_dag.util.command as command
import json


class PipelineStepGenerateTaxidLocator(PipelineStep):
    def run(self):
        # Setup
        input_fa = self.input_files_local[0][0]
        out_files = self.output_files_local()
        tmp = os.path.join(self.output_dir_local, "scratch")

        # Generate locator files for species NT, species NR, genus NT...
        for level in ["species", "genus", "family"]:
            for name in ("NT", "NR"):
                taxid_field = f"{level}_{name.lower()}"
                output_fa = out_files.pop(0)  # Pop from front
                output_json = out_files.pop(0)
                PipelineStepGenerateTaxidLocator.generate_locator_work(
                    input_fa, taxid_field, name, output_fa, output_json, tmp)

        # Cleanup
        command.execute("cd %s; rm -rf *" % tmp)

    @staticmethod
    def generate_locator_work(input_fa, taxid_field, hit_type, output_fa,
                              output_json, tmp):
        taxid_field_num = PipelineStepGenerateTaxidLocator.get_taxid_field_num(
            taxid_field, input_fa)
        # Put every 2-line fasta record on a single line with delimiter
        # ":lineseparator:":
        cmd = "awk 'NR % 2 == 1 { o=$0 ; next } "
        cmd += "{ print o \":lineseparator:\" $0 }' " + input_fa
        # Sort the records based on the field containing the taxids
        cmd += f" | sort -T {tmp} --key {taxid_field_num} "
        cmd += "--field-separator ':' --numeric-sort"
        # Split every record back over 2 lines
        cmd += f" | sed 's/:lineseparator:/\\n/g' > {output_fa}"
        command.execute(cmd)

        # Make JSON file giving the byte range of the file corresponding to each
        # taxid
        taxon_seq_locations = []
        out_f = open(output_fa, 'rb')
        seq_name = out_f.readline()
        seq_data = out_f.readline()

        taxid = PipelineStepGenerateTaxidLocator.get_taxid(
            seq_name, taxid_field)
        first_byte = 0
        end_byte = first_byte + len(seq_name) + len(seq_data)
        while len(seq_name) > 0 and len(seq_data) > 0:
            seq_name = out_f.readline()
            seq_data = out_f.readline()
            new_taxid = PipelineStepGenerateTaxidLocator.get_taxid(
                seq_name, taxid_field)
            summ = len(seq_name) + len(seq_data)
            if new_taxid != taxid:
                # Note on boundary condition: when end of file is reached, then
                # seq_name == '' => new_taxid == 'none' => new_taxid != taxid
                # so last record will be written to output correctly.
                taxon_seq_locations.append({
                    'taxid': int(taxid),
                    'first_byte': first_byte,
                    'last_byte': end_byte - 1,
                    'hit_type': hit_type
                })
                taxid = new_taxid
                first_byte = end_byte
                end_byte = first_byte + summ
            else:
                end_byte += summ
        out_f.close()

        with open(output_json, 'w') as out_f:
            json.dump(taxon_seq_locations, out_f)

    @staticmethod
    def get_taxid_field_num(taxid_field, input_fasta):
        with open(input_fasta) as f:
            seq_name = f.readline()
        return seq_name.replace('>', ':').split(":").index(taxid_field) + 1

    @staticmethod
    def get_taxid(seq_name, taxid_field):
        parts = seq_name.decode('utf-8').replace('>',
                                                 ':').split(f":{taxid_field}:")
        if len(parts) <= 1:
            # Sequence_name empty or taxid_field not found
            return 'none'
        taxid = parts[1].split(":")[0]
        # Example sequence_name: ">nr:-100:nt:684552:NR::NT:LT629734.1:HWI-ST640
        # :828:H917FADXX:2:1101:1424:15119/1"
        return taxid
