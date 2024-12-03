import * as monaco from 'monaco-editor';
import {KeyCode, KeyMod} from "monaco-editor";

export class MonacoEditor {
    private id: string;
    private initialText: string;
    private lang: string;
    private editor: monaco.editor.IStandaloneCodeEditor = null;
    private action: (arg: string) => void;

    constructor(id: string, lang: string, initialText: string, action: (arg:string) => void) {
        this.id = id;
        this.lang = lang;
        this.initialText = initialText;
        this.action = action;
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
                {token: 'predefined.sql', foreground: 'cc99ee'},
                {token: 'type.string', foreground: 'f8f8f2'},
                {token: 'string', foreground: 'f4c099'},
                {token: 'string.backquoted', foreground: 'f4c0cc'},
                {token: 'string.sql', foreground: 'f4c099'},
                {token: 'comment', foreground: '7282a4'},
                {token: 'operator', foreground: 'bd93f9'},
                {token: 'invalid', foreground: 'ff0000'},
            ]
        })

        // Use relative font size (rem) to adjust the editor font size
        var fontSize = 0.8 * parseFloat(getComputedStyle(document.documentElement).fontSize);

        this.editor = monaco.editor.create(document.getElementById(this.id), {
            value: this.initialText,
            language: this.lang,
            theme: theme,
            fontSize: fontSize,
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

        // Add shortcut key commands
        this.editor.addAction({
            id : "describe-query",
            label : "Describe Query",
            keybindings : [monaco.KeyMod.chord(
                monaco.KeyMod.WinCtrl | KeyCode.KeyJ,
                monaco.KeyMod.WinCtrl | KeyCode.KeyD
            )],
            run : async (editor, args) => {
                this.action('describe-query')
            },
        })

        this.editor.addAction({
            id : "run-subquery",
            label : "Run subquery",
            keybindings : [
                monaco.KeyMod.Shift | KeyCode.Enter
            ],
            run : async (editor, args) => {
                this.action('run-subquery')
            },
        })

        this.editor.addAction({
            id: "run-query",
            label: "Run Query",
            keybindings: [
                monaco.KeyMod.WinCtrl | KeyCode.Enter,
                KeyMod.chord(
                    monaco.KeyMod.WinCtrl | KeyCode.KeyJ,
                    monaco.KeyMod.WinCtrl | KeyCode.KeyR
                )
            ],
            run : async (editor, args) => {
                this.action('run-query')
            },
        })

        this.editor.addAction({
            id: "run-production-query",
            label: "Run query with production mode",
            keybindings: [
                monaco.KeyMod.CtrlCmd | KeyCode.Enter,
            ],
            run : async (editor, args) => {
                this.action('run-production-query')
            },
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
    }

    getLinePosition(): number {
        return this.editor.getPosition().lineNumber;
    }

    getColumnPosition(): number {
        return this.editor.getPosition().column;
    }

}
