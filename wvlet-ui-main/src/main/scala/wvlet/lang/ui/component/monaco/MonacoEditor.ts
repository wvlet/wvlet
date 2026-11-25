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

    // Light and dark color themes per language: vs-wvlet / vs-wvlet-light, vs-sql / vs-sql-light
    private static defineThemes(): void {
        const rules = [
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
        ];
        const lightRules = [
            {token: 'keyword', foreground: '0b6bcb'},
            {token: 'identifier', foreground: '1f2328'},
            {token: 'type.identifier', foreground: '5a4fcf'},
            {token: 'type.keyword', foreground: '8a3ffc'},
            {token: 'predefined.sql', foreground: '8a3ffc'},
            {token: 'type.string', foreground: '1f2328'},
            {token: 'string', foreground: 'b35900'},
            {token: 'string.backquoted', foreground: 'b3366b'},
            {token: 'string.sql', foreground: 'b35900'},
            {token: 'comment', foreground: '6a737d'},
            {token: 'operator', foreground: '7c3aed'},
            {token: 'invalid', foreground: 'd00000'},
        ];
        for (const name of ['vs-wvlet', 'vs-sql']) {
            monaco.editor.defineTheme(name, {
                base: 'vs-dark',
                inherit: true,
                colors: {'editor.background': '#202124'},
                rules: rules
            });
            monaco.editor.defineTheme(`${name}-light`, {
                base: 'vs',
                inherit: true,
                colors: {'editor.background': '#ffffff'},
                rules: lightRules
            });
        }
    }

    private static darkMode: boolean = true;

    private themeName(): string {
        const base = this.lang == 'wvlet' ? 'vs-wvlet' : 'vs-sql';
        return MonacoEditor.darkMode ? base : `${base}-light`;
    }

    /** Switch every editor between the dark and light themes */
    setDarkMode(dark: boolean): void {
        MonacoEditor.darkMode = dark;
        monaco.editor.setTheme(this.themeName());
    }

    /** Change the language used for syntax highlighting */
    setLanguage(lang: string): void {
        this.lang = lang;
        if (this.editor) {
            monaco.editor.setModelLanguage(this.editor.getModel(), lang);
            monaco.editor.setTheme(this.themeName());
        }
    }

    focus(): void {
        if (this.editor) {
            this.editor.focus();
        }
    }

    render(): void {
        MonacoEditor.defineThemes();
        const theme: string = this.themeName();

        // Use relative font size (rem) to adjust the editor font size
        var fontSize = 0.80 * parseFloat(getComputedStyle(document.documentElement).fontSize);

        this.editor = monaco.editor.create(document.getElementById(this.id), {
            value: this.initialText,
            language: this.lang,
            theme: theme,
            fontSize: fontSize,
            fontFamily: 'Monaco, Consolas, ui-monospace, SFMono-Regular, Menlo, monospace',
            minimap: {
                enabled: false
            },
            scrollbar: {
                horizontal: 'hidden',
                // Show the slider only while the pointer is over the editor
                vertical: 'auto',
                verticalScrollbarSize: 8,
                useShadows: false,
            },
            // No overview ruler strip on the right edge
            overviewRulerLanes: 0,
            overviewRulerBorder: false,
            hideCursorInOverviewRuler: true,
            wordWrapBreakAfterCharacters: '.,;',
            automaticLayout: true,
            tabSize: 2
        })

        // Add shortcut key commands
        this.editor.addAction({
            id : "describe-query",
            label : "Describe Query",
            keybindings : [
                monaco.KeyMod.Shift | KeyCode.Enter,
                monaco.KeyMod.chord(
                    monaco.KeyMod.WinCtrl | KeyCode.KeyJ,
                    monaco.KeyMod.WinCtrl | KeyCode.KeyD
                ),
            ],
            run : async (editor, args) => {
                this.action('describe-query')
            },
        })

        this.editor.addAction({
            id : "run-subquery",
            label : "Run subquery",
            keybindings : [
                monaco.KeyMod.WinCtrl | KeyCode.Enter,
            ],
            run : async (editor, args) => {
                this.action('run-subquery')
            },
        })

        this.editor.addAction({
            id: "run-query",
            label: "Run Query",
            keybindings: [
                monaco.KeyMod.CtrlCmd | KeyCode.Enter,
                monaco.KeyMod.chord(
                    monaco.KeyMod.WinCtrl | KeyCode.KeyJ,
                    monaco.KeyMod.WinCtrl | KeyCode.KeyR
                ),
            ],
            run : async (editor, args) => {
                this.action('run-query')
            },
        })

        this.editor.addAction({
            id: "run-production-query",
            label: "Run query with production mode",
            keybindings: [
                monaco.KeyMod.Shift | KeyMod.CtrlCmd | KeyCode.Enter,
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
            this.adjustSize(w, h);
        }
    }

    adjustSize(newWidth: number, newHeight: number): void {
        if (this.editor) {
            this.editor.layout({
                width: newWidth,
                height: newHeight
            });
        }
    }

    getLinePosition(): number {
        return this.editor.getPosition().lineNumber;
    }

    getColumnPosition(): number {
        return this.editor.getPosition().column;
    }

    enableWordWrap(): void {
        this.editor.updateOptions({
            wordWrap: 'on'
        });
    }
}
