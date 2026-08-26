| Module                                                                                                    | Responsibility                                  | Knows about                                        |
| --------------------------------------------------------------------------------------------------------- | ----------------------------------------------- | -------------------------------------------------- |
| [handlers.py](/Users/witold/dev/euphrosyne-ecosystem/euphrosyne-tools-api/data_visualization/handlers.py) | Understand and prepare specific data formats    | TRAUPIXE, file paths, normalization                |
| [llm.py](/Users/witold/dev/euphrosyne-ecosystem/euphrosyne-tools-api/data_visualization/llm.py)           | Define the provider-independent LLM contract    | Messages, tools, completions                       |
| [service.py](/Users/witold/dev/euphrosyne-ecosystem/euphrosyne-tools-api/data_visualization/service.py)   | Orchestrate the complete visualization use case | Prepared data, LLM calls, Python sessions, retries |

### Handlers: “What kind of data is this?”

A handler converts format-specific input into the generic representation required by the service.

For example, `TraupixeVisualizationHandler`:

1. Recognizes a TRAUPIXE file.
2. Parses its Excel structure.
3. Produces normalized content and a descriptor.
4. Adds TRAUPIXE-specific calculation and visualization instructions.

Its output is `PreparedDataVisualization`, which allows the rest of the system to remain unaware of Excel or TRAUPIXE details.

When adding CSV, HDF5, or another scientific format, this is primarily where the new implementation belongs.

### LLM: “How do I talk to a language model?”

`llm.py` defines only the interface:

```python
class DataVisualizationLlmClient(Protocol):
    def complete(...) -> DataVisualizationCompletion: ...
```

It deliberately does not know:

- What TRAUPIXE is.
- Why Python is being executed.
- How many retries are allowed.
- How visualizations are validated.

`AlbertClient` implements this protocol. Another provider could replace Albert without changing the visualization service.

### Service: “How is a visualization produced?”

`DataVisualizationService` coordinates the workflow:

```text
Prepared data
    → upload to Python session
    → ask LLM for calculation code
    → execute code
    → retrieve calculation result
    → ask LLM for ECharts options
    → validate and return visualizations
```

It owns application-level policies such as:

- Maximum file/result sizes.
- Number of Python and visualization attempts.
- Session creation and cleanup.
- Generic prompts and structured-response schemas.
- LLM usage tracking and exchange logging.

It accepts already-prepared data, so it remains independent of TRAUPIXE.

In practical terms:

- New file format → add a handler.
- New LLM provider → implement the LLM protocol.
- Change the visualization workflow → modify the service.
