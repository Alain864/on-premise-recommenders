import { FormEvent, useEffect, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import { fetchAutocomplete } from "../api";
import type { AutocompleteSuggestion } from "../types";
import { useUserId } from "../user";

export function SearchBar() {
  const [query, setQuery] = useState("");
  const [open, setOpen] = useState(false);
  const [suggestions, setSuggestions] = useState<AutocompleteSuggestion[]>([]);
  const navigate = useNavigate();
  const { userId } = useUserId();
  const blurTimer = useRef<number | null>(null);

  useEffect(() => {
    if (query.trim().length < 2) {
      setSuggestions([]);
      return;
    }
    const handle = window.setTimeout(() => {
      fetchAutocomplete(query.trim(), userId || null)
        .then((data) => setSuggestions(data.suggestions))
        .catch(() => setSuggestions([]));
    }, 150);
    return () => window.clearTimeout(handle);
  }, [query, userId]);

  function go(value: string) {
    if (!value.trim()) return;
    navigate(`/search?q=${encodeURIComponent(value.trim())}`);
    setOpen(false);
  }

  function onSubmit(event: FormEvent) {
    event.preventDefault();
    go(query);
  }

  return (
    <form className="search-form" onSubmit={onSubmit}>
      <div className="search-container">
        <input
          className="search-input"
          placeholder="Search products..."
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          onFocus={() => setOpen(true)}
          onBlur={() => {
            blurTimer.current = window.setTimeout(() => setOpen(false), 200);
          }}
        />
        <button className="search-button" type="submit">
          Search
        </button>
      </div>
      {open && query.trim().length >= 2 && (
        <div className="suggestions-dropdown">
          {suggestions.length === 0 ? (
            <div className="no-suggestions">No suggestions</div>
          ) : (
            <ul className="suggestions-list">
              {suggestions.map((item) => (
                <li
                  key={item.query_text}
                  className="suggestion-item"
                  onMouseDown={(event) => {
                    event.preventDefault();
                    setQuery(item.query_text);
                    go(item.query_text);
                  }}
                >
                  <span className="suggestion-text">{item.query_text}</span>
                  {item.category_match && (
                    <span className="suggestion-category">{item.category_match}</span>
                  )}
                </li>
              ))}
            </ul>
          )}
        </div>
      )}
    </form>
  );
}
