import base64


def db_decode(text):
    b64_byte = text.encode('ascii')
    m_bytes = base64.b64decode(b64_byte)
    return m_bytes.decode('ascii').replace('\n', '')