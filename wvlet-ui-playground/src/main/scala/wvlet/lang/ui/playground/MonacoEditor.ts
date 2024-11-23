import * as monaco from 'monaco-editor';

export class MonacoEditor {
    private id: string;
    private initialText: string;
    private lang: string;
    private editor: monaco.editor.IStandaloneCodeEditor = null;

    constructor(id: string, lang: string, initialText: string) {
        this.id = id;
        this.lang = lang;
        this.initialText = initialText;
    }

    hello(): void {
        console.log("Hello, MonacoEditor");
    }

    render(): void {
        const theme: string = this.lang == 'wvlet' ? 'vs-wvlet' : 'vs-sql';

        monaco.editor.defineTheme(theme, {
            base: 'vs-dark',
            inherit: true,
            colors: {
                'editor.background': '#202124',
            },
            rules: [
                {token: 'keyword', foreground: '58ccf0'},
                {token: 'identifier', foreground: 'f8f8f2'},
                {token: 'type.identifier', foreground: 'aaaaff'},
                {token: 'type.keyword', foreground: 'cc99cc'},
                {token: 'type.string', foreground: 'f8f8f2'},
                {token: 'string', foreground: 'f4c099'},
                {token: 'string.backquoted', foreground: 'f4c0cc'},
                {token: 'string.sql', foreground: 'f4c099'},
                {token: 'comment', foreground: '6272a4'},
                {token: 'operator', foreground: 'bd93f9'},
                {token: 'invalid', foreground: 'ff0000'},
            ]
        })



        this.editor = monaco.editor.create(document.getElementById(this.id), {
            value: this.initialText,
            language: this.lang,
            theme: theme,
            fontSize: 12,
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

    getText(): string {
        return this.editor.getValue();
    }

    setText(newText: string): void {
        this.editor.setValue(newText)
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
