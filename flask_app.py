from flask import Flask
from flask import request
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


@app.route('/documents/index', methods=['GET', 'POST'])
def index_doc():
    doc = (request.args.get('title'), request.args.get('author'), request.args.get('content'))
    Injector().get(Manager).index_document(doc)
    return 'Document successfully indexed'


@app.route('/search', methods=['GET', 'POST'])
def search():
    if request.method == 'GET':
        phrase = request.args.get('phrase')
        case_sensitive = request.args.get('case')
    else:
        phrase = request.get_json().get('phrase')
        case_sensitive = request.get_json().get('case')
    if not phrase:
        return 'phrase parameter not exists'

    return Injector().get(Manager).search(phrase, case_sensitive)


@app.route('/exact', methods=['GET', 'POST'])
def exact():
    if request.method == 'GET':
        phrase = request.args.get('phrase')
    else:
        phrase = request.get_json().get('phrase')
    if not phrase:
        return 'phrase parameter not exists'

    return Injector().get(Manager).exact(phrase)


@app.before_first_request
def before_first_request():
    Injector().get(Manager).create_session()


if __name__ == '__main__':
    app.run()
