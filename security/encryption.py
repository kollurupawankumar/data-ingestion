# encryption.py
from cryptography.fernet import Fernet


class Decryptor:
    def __init__(self, key: str):
        self.cipher = Fernet(key)

    def decrypt_file(self, file_path: str) -> bytes:
        with open(file_path, 'rb') as f:
            return self.cipher.decrypt(f.read())