import hashlib


def remove_stopwords(sentence,stopword_list):
    for stopword in stopword_list:
        sentence = sentence.replace(stopword,' ')
    return sentence.strip()


def get_sha_hash(content):
    sha = hashlib.sha1()
    sha.update(content)
    return sha.hexdigest()
