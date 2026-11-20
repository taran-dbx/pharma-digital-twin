import { Plus, Trash2 } from "lucide-react";
import { Button } from "@/components/ui/button";

export interface KeyValuePair {
  key: string;
  value: string;
}

interface KeyValueEditorProps {
  pairs: KeyValuePair[];
  onChange: (pairs: KeyValuePair[]) => void;
  keyPlaceholder?: string;
  valuePlaceholder?: string;
  keyLabel?: string;
  valueLabel?: string;
  addLabel?: string;
  readOnlyKeys?: Set<string>;
}

export function KeyValueEditor({
  pairs,
  onChange,
  keyPlaceholder = "Key",
  valuePlaceholder = "Value",
  keyLabel = "Key",
  valueLabel = "Value",
  addLabel = "Add",
  readOnlyKeys,
}: KeyValueEditorProps) {
  const update = (index: number, field: "key" | "value", val: string) => {
    const next = pairs.map((p, i) =>
      i === index ? { ...p, [field]: val } : p
    );
    onChange(next);
  };

  const remove = (index: number) => {
    onChange(pairs.filter((_, i) => i !== index));
  };

  const add = () => {
    onChange([...pairs, { key: "", value: "" }]);
  };

  return (
    <div className="space-y-2">
      {pairs.length > 0 && (
        <div className="grid grid-cols-[1fr_1fr_32px] gap-2 px-1">
          <span className="text-[10px] font-medium uppercase tracking-wider text-muted-foreground">
            {keyLabel}
          </span>
          <span className="text-[10px] font-medium uppercase tracking-wider text-muted-foreground">
            {valueLabel}
          </span>
          <span />
        </div>
      )}
      {pairs.map((pair, i) => {
        const isReadOnly = readOnlyKeys?.has(pair.key);
        return (
          <div key={i} className="grid grid-cols-[1fr_1fr_32px] gap-2">
            <input
              type="text"
              value={pair.key}
              readOnly={isReadOnly}
              onChange={(e) => update(i, "key", e.target.value)}
              placeholder={keyPlaceholder}
              className="rounded-md border border-input bg-background px-2.5 py-1.5 font-mono text-xs placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-ring disabled:opacity-50 read-only:bg-muted"
            />
            <input
              type="text"
              value={pair.value}
              onChange={(e) => update(i, "value", e.target.value)}
              placeholder={valuePlaceholder}
              className="rounded-md border border-input bg-background px-2.5 py-1.5 font-mono text-xs placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-ring"
            />
            {isReadOnly ? (
              <div />
            ) : (
              <Button
                variant="ghost"
                size="icon"
                className="h-7 w-7 text-muted-foreground hover:text-destructive"
                onClick={() => remove(i)}
                aria-label="Remove row"
              >
                <Trash2 className="h-3.5 w-3.5" />
              </Button>
            )}
          </div>
        );
      })}
      <Button
        variant="outline"
        size="sm"
        className="gap-1.5 text-xs"
        onClick={add}
      >
        <Plus className="h-3 w-3" />
        {addLabel}
      </Button>
    </div>
  );
}
