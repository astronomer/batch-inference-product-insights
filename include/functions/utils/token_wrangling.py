import tiktoken

def count_tokens(text, model="gpt-4o"):
    enc = tiktoken.encoding_for_model(model)
    return len(enc.encode(text))

def split_text_to_fit_tokens(text, max_tokens, model="gpt-4o"):
    enc = tiktoken.encoding_for_model(model)
    tokens = enc.encode(text)
    chunks = []
    
    while len(tokens) > max_tokens:
        chunk = tokens[:max_tokens]
        chunks.append(enc.decode(chunk))
        tokens = tokens[max_tokens:]
    
    chunks.append(enc.decode(tokens))
    return chunks


def format_feedback(feedback_dict):
    formatted_feedback = ""
    separator = "\n--------\n\n"
    
    for user, feedback in feedback_dict.items():
        formatted_feedback += f"{user}:\n{feedback}\n{separator}"
    
    formatted_feedback = formatted_feedback.rstrip(separator)
    
    return formatted_feedback


def add_line_breaks(text, max_length):
    def break_text(t):
        lines = []
        for i in range(0, len(t), max_length):
            lines.append(t[i:i + max_length])
        return '\n'.join(lines)
    
    if isinstance(text, list):
        return [break_text(t) for t in text]
    else:
        return break_text(text)

