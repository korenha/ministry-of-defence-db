import shutil, os


def copytree2(source, dest):
    if not dest.is_file():
        os.mkdir(dest)
    dest_dir = os.path.join(dest, os.path.basename(source))
    shutil.copytree(source.name, dest_dir)
