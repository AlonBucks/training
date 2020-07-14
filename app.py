from app import manager
from flask import Flask, request

app = Flask(__name__)


@app.route('/init', methods=['GET', 'POST'])
def init():
    manager.init()
    return 'App successfully initialized'


@app.route('/index', methods=['GET', 'POST'])
def index():
    manager.run_index_async()
    return 'Indexing all documents'


@app.route('/index_doc', methods=['GET', 'POST'])
def index_doc():
    doc = (request.args.get('title'), request.args.get('author'), request.args.get('content'))
    manager.index_document(doc)
    return 'Document successfully indexed'


@app.route('/search')
def search():
    return manager.search(request.args.get('p1'), request.args.get('p2'))


@app.route('/exact')
def exact():
    return manager.exact(request.args.get('p1'))


if __name__ == '__main__':
    app.run(host='0.0.0.0')
