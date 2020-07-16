from flask import Flask, request
from injector import Injector
from app.manager import Manager

app = Flask(__name__)


@app.route('/init', methods=['GET', 'POST'])
def init():
    Injector().get(Manager).init()
    return 'App successfully initialized'


@app.route('/index', methods=['GET', 'POST'])
def index():
    Injector().get(Manager).run_index_async()
    return 'Indexing all documents'


@app.route('/index_doc', methods=['GET', 'POST'])
def index_doc():
    doc = (request.args.get('title'), request.args.get('author'), request.args.get('content'))
    Injector().get(Manager).index_document(doc)
    return 'Document successfully indexed'


@app.route('/search')
def search():
    phrase = request.args.get('phrase')
    case_sensitive = request.args.get('case')
    if not phrase:
        return 'phrase parameter not exists'

    return Injector().get(Manager).search(phrase, case_sensitive)


@app.route('/exact')
def exact():
    phrase = request.args.get('phrase')
    if not phrase:
        return 'phrase parameter not exists'

    return Injector().get(Manager).exact(phrase)


if __name__ == '__main__':
    app.run(host='0.0.0.0')
