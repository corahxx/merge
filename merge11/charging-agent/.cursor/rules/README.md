# Cursor Rules 规则说明

本目录的规则来自 [flyeric0212/cursor-rules](https://github.com/flyeric0212/cursor-rules)，已安装到当前项目中供 Cursor 使用。

## 目录结构

| 目录 | 说明 |
|------|------|
| **base/** | 通用规则（始终生效）：core、tech-stack、project-structure、general |
| **languages/** | 按文件类型生效：Python、TypeScript、Java、Go、CSS、C++、C#、Kotlin、WXML、WXSS 等 |
| **frameworks/** | 框架规则：React、Vue、Next.js、FastAPI、Django、Spring Boot、Android、Flutter 等 |
| **other/** | 可选规则，需用 `@` 引用：document、git、gitflow |

## 使用方式

1. **通用规则**：`base/` 下的规则会始终应用（如中文回复、项目风格）。
2. **语言/框架规则**：打开对应类型文件时，Cursor 会按 `globs` 自动匹配规则。
3. **可选规则**：在对话中用 `@` 引用，例如 `@document.mdc`、`@git.mdc`。

## 建议下一步

- 在 **base/tech-stack.mdc** 中填写本项目技术栈与文档链接。
- 在 **base/project-structure.mdc** 中补充本项目的目录与模块约定。

更新规则内容后无需重启，Cursor 会自动加载。
