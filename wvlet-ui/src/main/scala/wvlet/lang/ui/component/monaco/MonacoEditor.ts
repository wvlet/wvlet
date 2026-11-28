import * as monaco from 'monaco-editor';
import {KeyCode, KeyMod} from "monaco-editor";

export class MonacoEditor {
    private id: string;
    private initialText: string;
    private lang: string;
    private editor: monaco.editor.IStandaloneCodeEditor = null;
    private action: (arg: string) => void;
    private markers: monaco.editor.IEditorDecorationsCollection = null;

    constructor(id: string, lang: string, initialText: string, action: (arg:string) => void) {
        this.id = id;
        this.lang = lang;
        this.initialText = initialText;
        this.action = action;
    }

    private static languageServicesRegistered: boolean = false;

    /**
     * Register hover and completion for the `wvlet` language, once per page. Monaco providers are
     * global per language, so every Wvlet editor on the page gets them. `hover` returns markdown
     * and a 1-origin range, or null; `complete` returns LSP-style items (`kind` is an LSP
     * CompletionItemKind number). Both take the editor text and a 0-origin UTF-16 offset.
     */
    static registerLanguageServices(
        hover: (text: string, offset: number) => {content: string, startLine: number, startColumn: number, endLine: number, endColumn: number} | null,
        complete: (text: string, offset: number) => {label: string, kind: number, detail: string}[]
    ): void {
        if (MonacoEditor.languageServicesRegistered) {
            return;
        }
        MonacoEditor.languageServicesRegistered = true;
        // LSP CompletionItemKind -> Monaco's enum, for the kinds Wvlet completions use
        const K = monaco.languages.CompletionItemKind;
        const kinds: {[lsp: number]: monaco.languages.CompletionItemKind} = {
            3: K.Function, 5: K.Field, 6: K.Variable, 7: K.Class, 14: K.Keyword, 22: K.Struct,
        };
        monaco.languages.registerHoverProvider('wvlet', {
            provideHover: (model, position) => {
                const h = hover(model.getValue(), model.getOffsetAt(position));
                if (!h) {
                    return null;
                }
                return {
                    contents: [{value: h.content}],
                    range: new monaco.Range(h.startLine, h.startColumn, h.endLine, h.endColumn),
                };
            }
        });
        monaco.languages.registerCompletionItemProvider('wvlet', {
            triggerCharacters: ['.'],
            provideCompletionItems: (model, position) => {
                const word = model.getWordUntilPosition(position);
                const range = new monaco.Range(
                    position.lineNumber, word.startColumn, position.lineNumber, word.endColumn
                );
                const items = complete(model.getValue(), model.getOffsetAt(position));
                return {
                    suggestions: items.map(i => ({
                        label: i.label,
                        kind: kinds[i.kind] ?? K.Text,
                        detail: i.detail,
                        insertText: i.label,
                        range: range,
                    }))
                };
            }
        });
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

    /** Scroll to a 1-origin line, select it, and focus the editor */
    revealLine(line: number): void {
        if (this.editor) {
            const model = this.editor.getModel();
            const n = Math.max(1, Math.min(line, model.getLineCount()));
            this.editor.revealLineInCenter(n);
            this.editor.setSelection(new monaco.Selection(n, 1, n, model.getLineMaxColumn(n)));
            this.editor.focus();
        }
    }

    /**
     * Show a marker in the glyph margin of each given 1-origin line, with its message on hover.
     * `kind` names the CSS class `wv-line-marker--<kind>` that draws the marker. An empty list
     * clears the markers; the margin appears with the first marker and then stays, so the text
     * doesn't shift on every edit.
     */
    setLineMarkers(markers: {line: number, kind: string, message: string}[]): void {
        if (!this.editor) {
            return;
        }
        if (markers.length > 0) {
            this.editor.updateOptions({glyphMargin: true});
        }
        const fence = '```';
        const decorations = markers.map(m => ({
            range: new monaco.Range(m.line, 1, m.line, 1),
            options: {
                glyphMarginClassName: `wv-line-marker wv-line-marker--${m.kind}`,
                glyphMarginHoverMessage: {value: `${fence}\n${m.message.replaceAll(fence, "'''")}\n${fence}`},
            }
        }));
        if (this.markers) {
            this.markers.set(decorations);
        } else {
            this.markers = this.editor.createDecorationsCollection(decorations);
        }
    }

    focus(): void {
        if (this.editor) {
            this.editor.focus();
        }
    }

    /**
     * Size the editor to its text, between `min` and `max` pixels, for an editor in a page that
     * scrolls (a notebook cell). The mouse wheel scrolls the page unless the editor itself has
     * more to scroll.
     */
    enableAutoHeight(min: number, max: number): void {
        if (!this.editor) {
            return;
        }
        this.editor.updateOptions({
            scrollBeyondLastLine: false,
            scrollbar: {alwaysConsumeMouseWheel: false, vertical: 'auto', horizontal: 'auto'},
        });
        const container = document.getElementById(this.id);
        const update = () => {
            const h = Math.min(max, Math.max(min, this.editor.getContentHeight()));
            container.style.height = `${h}px`;
            this.editor.layout({width: container.clientWidth, height: h});
        };
        this.editor.onDidContentSizeChange(update);
        update();
    }

    /** Release the editor, e.g. when its page or notebook cell goes away */
    dispose(): void {
        if (this.editor) {
            this.editor.dispose();
            this.editor = null;
            this.markers = null;
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

        this.markers = null;
        // Tell the page which editor has the focus, e.g. to select its notebook cell
        this.editor.onDidFocusEditorText(() => this.action('focus'));

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

    getSelectedText(): string {
        const sel = this.editor.getSelection();
        if (!sel || sel.isEmpty()) {
            return "";
        }
        return this.editor.getModel()?.getValueInRange(sel) ?? "";
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
