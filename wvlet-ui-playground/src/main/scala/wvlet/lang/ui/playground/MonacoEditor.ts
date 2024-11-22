import * as monaco from 'monaco-editor';

export class MonacoEditor {
    private editor: monaco.editor.IStandaloneCodeEditor = null;

    constructor() {
    }

    hello(): void {
        console.log("Hello, MonacoEditor");
    }

    renderTo(id:string): void {
        this.editor = monaco.editor.create(document.getElementById(id), {
            value: 'SELECT 1',
            language: 'sql',
            theme: 'vs-dark',
            fontSize: 13,
            fontFamily: 'Consolas, ui-monospace, SFMono-Regular, Menlo, Monaco, monospace',
            minimap: {
                enabled: false
            },
            scrollbar: {
                horizontal: 'hidden',
            },
            automaticLayout: true,
            tabSize: 2.0,
        })
    };

    adjustSize(): void {
        if (this.editor) {
            const w = this.editor.getContentWidth();
            const h = window.outerHeight - 512 - 44;
            this.editor.layout({
                width: w,
                height: h
            });
        }
    };
}
