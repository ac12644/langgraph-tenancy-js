/** Shared graph fixture used by every test file. */

import { AIMessage } from "@langchain/core/messages";
import type { RunnableConfig } from "@langchain/core/runnables";
import type {
  BaseCheckpointSaver,
  BaseStore,
} from "@langchain/langgraph-checkpoint";
import {
  Annotation,
  END,
  START,
  StateGraph,
  type LangGraphRunnableConfig,
} from "@langchain/langgraph";

import { getTenantStore } from "../src/index.js";

export const State = Annotation.Root({
  messages: Annotation<unknown[]>({
    reducer: (a, b) => a.concat(b),
    default: () => [],
  }),
});

export function makeGraph(options: {
  checkpointer: BaseCheckpointSaver;
  store?: BaseStore;
  rawStoreAccess?: boolean;
  modelName?: string;
}) {
  const { checkpointer, store, rawStoreAccess, modelName } = options;
  const agent = async (
    state: typeof State.State,
    config: LangGraphRunnableConfig
  ) => {
    // real chat models always set an id; the ledger dedupes on it
    const reply = new AIMessage({
      id: crypto.randomUUID(),
      content: `echo: ${state.messages.at(-1)}`,
      usage_metadata: { input_tokens: 10, output_tokens: 5, total_tokens: 15 },
      response_metadata: { model_name: modelName ?? "claude-sonnet-4-6" },
    });
    if (store) {
      if (rawStoreAccess) {
        // bypass tenancy on purpose — must fail closed
        await config.store!.put(["memories"], "leak", { oops: true });
      } else {
        const tenantStore = getTenantStore(config);
        await tenantStore.put(["memories"], `note-${state.messages.length}`, {
          last: String(state.messages.at(-1)),
        });
      }
    }
    return { messages: [reply] };
  };
  return new StateGraph(State)
    .addNode("agent", agent)
    .addEdge(START, "agent")
    .addEdge("agent", END)
    .compile({ checkpointer, store });
}

export const cfg = (tenant: string, thread = "t1"): RunnableConfig => ({
  configurable: { thread_id: thread, tenant_id: tenant },
});
