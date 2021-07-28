import glob
import os
import subprocess

# Get only samples not run yet:
list_patients_jb = os.listdir("/cephfs/PROJECTS/jbreynier/Nigerian/WGS")
list_bams_all_normal = glob.glob("/cephfs/PROJECTS/WABCS/Tumor-Normal/BAM/*/WGS/Normal/*.bam")
list_patients_all = [bam_path.split("/")[-4] for bam_path in list_bams_all_normal]
list_patients_link = [sample for sample in list_patients_all if sample not in list_patients_jb]

project_dir = "/cephfs/PROJECTS/jbreynier/Nigerian/WGS"
input_dir = "/cephfs/PROJECTS/WABCS/Tumor-Normal"

links = []
for patient_id in list_patients_link:
    path = os.path.join(input_dir, patient_id, "WGS")
    os.makedirs(os.path.join(project_dir, patient_id), exist_ok=True)
    for tag, ext in [
        ("Normal", ".bam"),
        ("Normal", ".bai")
    ]:
        print(os.path.join(path, tag, "*" + ext))
        if len(glob.glob(os.path.join(path, tag, "*" + ext))) > 0 :
            target = glob.glob(os.path.join(path, tag, "*" + ext))[0]
            basename = os.path.basename(target).split(".")[0]
            link = os.path.join(
                project_dir, patient_id, tag.lower() + '.' + basename + ".final" + ext
            )
            if ext == ".bam":
                links += [link]
            if not os.path.isfile(link):
                print("linking ", target, link)
                print("ln {} {}".format(target, link))
                subprocess.check_output("ln {} {}".format(target, link), shell=True)
            if ext == '.bai':
                try:
                    # tools complain if index is older than bam-- it is concerning
                    # but for now, hack it
                    print("touch {}".format(link))
                    subprocess.check_output("touch {}".format(link), shell=True)
                except Exception as e:
                    print(e)

normals = [l for l in links if 'normal' in l]
print(len(links))
print(len(normals))
