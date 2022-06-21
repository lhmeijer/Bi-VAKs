import tarfile
import os

if __name__ == "__main__":
    raw_version_data_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'BEAR-A-raw',
                                        'BEAR-A-alldata.CB.nt')

    raw_version_data_file_name = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'BEAR-A-raw',
                                        'alldata.IC.nt.tar.gz')

    file = tarfile.open(raw_version_data_file_name)
    # print(file.getnames())
    file.extract('000001.nt.gz', raw_version_data_dir)
    file.close()