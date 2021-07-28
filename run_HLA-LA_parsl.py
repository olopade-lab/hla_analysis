import os
import importlib
import argparse
from wcmatch import glob as wcmatchglob
import glob
from parsl import wait_for_current_tasks as parsl_wait_for_current_tasks
from parsl import bash_app
import exceptions
import helper_functions

def main():
    parser = argparse.ArgumentParser(description='''Script to run HLA-LA with Parsl''')
    parser.add_argument('-b', '--bam', type=str, required=True, help='''Path of the input bam file(s)''')
    parser.add_argument('-o', '--out', type=str, required=True, help='''Path of the output directory''')                                                                         
    parser.add_argument('-t', '--threads', type=int, required=False, default=6, help='''Number of threads for running HLA-LA''')
    parser.add_argument('-c', '--config', type=str, default='dolores_HLA-LA', help='''Config file to use for running Parsl''')
    parser.add_argument('-g', '--graph', type=str, required=True, help='''Path to graph directory''')
    args = parser.parse_args()
    helper_functions.load_config(args.config)

    if not os.path.isdir(args.out):
        raise exceptions.IncorrectPathError(args.out)
    if not os.path.isdir(args.graph):
        raise exceptions.IncorrectPathError(args.graph)
    if os.path.basename(os.path.normpath(args.graph)) != "PRG_MHC_GRCh38_withIMGT":
        raise exceptions.IncorrectPathError(args.graph)
    bam_path = args.bam
    try:
        base_dir = bam_path.rsplit("/", 2)[0] + "/"
        name_format = bam_path.rsplit("/", 1)[-1]
    except:
        raise exceptions.IncorrectPathError(bam_path)
    list_files = wcmatchglob.glob(bam_path, flags=wcmatchglob.BRACE)
    if not list_files:
        raise exceptions.IncorrectPathError(bam_path)
    list_samples = list(set([file.split("/")[-2] for file in list_files]))
    for sample in list_samples:
        bam_specific = wcmatchglob.glob(base_dir+sample+"/"+name_format, flags=wcmatchglob.BRACE)
        if len(bam_specific) > 1:
            print(f"More than 1 matching BAM file for sample {sample}. Sample ignored.")
        elif len(bam_specific) == 1:
            bam_file = bam_specific[0]
            specific_out = os.path.join(args.out, sample)
            if not os.path.isdir(specific_out):
                os.mkdir(specific_out)
            if len(glob.glob(os.path.join(specific_out, "hla","*_R1_bestguess_G.txt"))) == 0:
                if not os.path.isfile(bam_file.replace(".bam", ".bai")):
                    sam_future = SAM_index(bam_file, args.out)
                else:
                    sam_future = []
                HLA_typing(bam_file, args.out, args.threads, args.graph, inputs=sam_future)
    parsl_wait_for_current_tasks()
    
@bash_app    
def HLA_typing(bam, out, threads, graph, inputs=[]):
    import os
    import helper_functions
    sample_id = bam.split("/")[-2].replace("-", "_")
    HLA_script = ("docker run --rm -v {out}:/HLALA_out "
                    "-v {bam_dir}:/HLALA_in -v {graph}:/usr/local/bin/HLA-LA/graphs/PRG_MHC_GRCh38_withIMGT "
                    "zlskidmore/hla-la:latest HLA-LA.pl --BAM /HLALA_in/{bam_file} "
                    "--graph PRG_MHC_GRCh38_withIMGT "
                    "--sampleID {id} "
                    "--maxThreads {threads} "
                    "--workingDir /HLALA_out").format(bam_dir=os.path.dirname(bam),
                                                bam_file=os.path.basename(bam),
                                                graph=graph,
                                                id=sample_id,
                                                threads=threads,
                                                out=out)
    print(HLA_script)
    helper_functions.write_log("Running HLA-LA for {id}".format(id=sample_id), out)
    return HLA_script

@bash_app        
def SAM_index(bam, out):
    import helper_functions
    SAM_ind_script = ("samtools index "
                    "{bam}").format(bam=bam)
    helper_functions.write_log("Generating index file for {f}".format(f=bam), out)
    return SAM_ind_script

if __name__ == "__main__":
    main()