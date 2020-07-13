import manager
from flask import Flask
from flask import request

app = Flask(__name__)


@app.route('/')
def hello_world():
    return 'Hello World!'


@app.route('/init')
def init():
    manager.init()
    return "ok"


@app.route('/index')
def index():
    manager.run_index_async()
    return 'ok'


@app.route('/search')
def search():
    return manager.search(request.args.get('p1'), request.args.get('p2'))


@app.route('/exact')
def exact():
    return manager.exact(request.args.get('p1'))


if __name__ == '__main__':
    app.run(host='0.0.0.0')
