import pickle
import os
import struct
import mmap

class Answer:

    def __init__(self):
        self.wal_path = "./wal"
        self.storage_path = "./storage"
        self.maxlength = 100000
        self.storage_keys = 0
        self.cache = {}
        self.storages = []
        self.last_index = 0
        self.load_caches()
        self.wal_file = open(self.wal_path, 'a')
        return

    def load_caches(self):
        if not os.path.exists(self.storage_path):
            os.mkdir(self.storage_path)

        for f in os.listdir(self.storage_path):
            full_path = os.path.join(self.storage_path, f)
            if os.path.isfile(full_path):
                storage = Storage(full_path)
                storage.load()
                self.storages.append(storage)
                self.last_index = int(f)

        if not os.path.exists(self.wal_path):
            open(self.wal_path, 'a').close()

        if os.path.getsize(self.wal_path) > 0:
            with open(self.wal_path, "r+b") as wal_file:
                mm = mmap.mmap(wal_file.fileno(), 0, prot=mmap.PROT_READ)
                for line in iter(mm.readline, ""):
                    line = line.strip("\n")
                    key, value = line.split(' ')
                    self.cache[key] = value

    def get(self, key):
        if key in self.cache:
            return self.cache[key]
        else:
            length = len(self.storages)
            for i in range(length):
                value = self.storages[length - i - 1].lookup(key)
                if value is not None:
                    return value
        return 'NULL'

    def put(self, key, value):
        if len(self.cache) >= self.maxlength:
            print "maxlength: ", self.maxlength, "\n"
            self.last_index = self.last_index + 1
            full_path = os.path.join(self.storage_path, str(self.last_index))
            storage = Storage(full_path)
            storage.dump(self.cache)
            self.storages.append(storage)
            #self.caches.append(self.cache)
            self.cache = {}
            self.wal_file.seek(0)
            self.wal_file.truncate()
            self.storage_keys = self.storage_keys + self.maxlength
            self.maxlength = 100000 - self.storage_keys * 0.1

        self.wal_file.write("{} {}\n".format(key, value))
        self.cache[key] = value


class Storage:
    def __init__(self, path):
        self.path = path
        self.index = {}
        self.last = 0
        self.file = None

    def dump(self, cache):
        with open(self.path, 'wb') as f:
            for key in cache:
                value = cache[key]
                self.index[key] = self.last
                f.write(value+"\n")
                self.last = self.last + len(value) + 1
            dumps = pickle.dumps(self.index)
            f.write(dumps)
            f.write(struct.pack('i', self.last))

        self.file = open(self.path, 'r')

    def load(self):
        size = os.path.getsize(self.path)
        self.file = open(self.path, 'r')
        last_offset = size - 4
        self.file.seek(last_offset)
        self.last = struct.unpack('i', self.file.read(4))
        self.last = self.last[0]
        self.file.seek(self.last)
        string = self.file.read(last_offset - self.last)
        self.index = pickle.loads(string)

    def lookup(self, key):
        if key in self.index:
            offset = self.index[key]
            self.file.seek(offset)
            value = self.file.readline()
            return value.strip('\n')
        else:
            return None
