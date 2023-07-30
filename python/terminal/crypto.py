from base64 import urlsafe_b64encode as encode, urlsafe_b64decode as decode

# noinspection PyPackageRequirements
import nacl.utils

# noinspection PyPackageRequirements
from nacl.public import PrivateKey, Box, PublicKey

# noinspection PyPackageRequirements
from nacl.secret import SecretBox


def public_box(pri_key: str, pub_key: str):
    pri_key = PrivateKey(decode(pri_key.encode("utf-8")))
    pub_key = PublicKey(decode(pub_key.encode("utf-8")))
    return Box(pri_key, pub_key)


def generate_secret_key():
    secret_key = nacl.utils.random(nacl.secret.SecretBox.KEY_SIZE)
    encoded = encode(secret_key)
    return encoded.decode("utf-8")


def secret_box(secret_key: str):
    secret_key = decode(secret_key.encode("utf-8"))
    return SecretBox(secret_key)


def encrypt(box: Box | SecretBox, message: str):
    encrypted = box.encrypt(message.encode("utf-8"))
    encoded = encode(encrypted)
    return encoded.decode("utf-8")


def decrypt(box: Box | SecretBox, message: str):
    decoded = decode(message.encode("utf-8"))
    decrypted = box.decrypt(decoded)
    return decrypted.decode("utf-8")
