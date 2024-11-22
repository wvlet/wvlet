import * as monaco from 'monaco-editor';

export class MonacoEditor {
    private id: string;
    private initialText: string;
    private lang: string;
    private editor: monaco.editor.IStandaloneCodeEditor = null;

    constructor(id:string, lang: string, initialText: string) {
        this.id = id;
        this.lang = lang;
        this.initialText = initialText;
    }

    hello(): void {
        console.log("Hello, MonacoEditor");
    }

    render(): void {
        this.editor = monaco.editor.create(document.getElementById(this.id), {
            value: this.initialText,
            language: this.lang,
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
            tabSize: 2
        })
    };

    setReadOnly(): void {
        // disable text edit
        this.editor.updateOptions({
            readOnly: true
        });
    }

    adjustHeight(newHeight: number): void {
        if (this.editor) {
            const w = this.editor.getLayoutInfo().width;
            const h = newHeight;
            this.editor.layout({
                width: w,
                height: h
            });
        }
    };
}
