import { parseCardContent } from './cardContent'
import './CardContentDisplay.css'

export default function CardContent({ text }: { text: string }): React.JSX.Element {
  return (
    <>
      {parseCardContent(text).map((block, i) => {
        if (block.type === 'code') {
          return (
            <pre key={i} className="cc-code">
              {block.text}
            </pre>
          )
        }
        if (block.type === 'bullet') {
          return (
            <div key={i} className="cc-bullet">
              <span className="cc-bullet-glyph">•</span>
              <span className="cc-bullet-text">{block.text}</span>
            </div>
          )
        }
        return (
          <div key={i} className="cc-text">
            {block.text || ' '}
          </div>
        )
      })}
    </>
  )
}
