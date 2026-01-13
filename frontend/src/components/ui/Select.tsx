import React from 'react'

interface SelectProps extends React.SelectHTMLAttributes<HTMLSelectElement> {
  label?: string
  error?: string
  helperText?: string
  options: Array<{ value: string; label: string }>
}

export function Select({
  label,
  error,
  helperText,
  options,
  className = '',
  ...props
}: SelectProps) {
  return (
    <div className="w-full">
      {label && (
        <label className="block text-sm font-medium text-databricks-gray-700 mb-1">
          {label}
          {props.required && <span className="text-databricks-error ml-1">*</span>}
        </label>
      )}
      <select
        className={`w-full px-3 py-2 border rounded-md text-sm transition-colors appearance-none
          bg-white bg-no-repeat bg-right
          ${error 
            ? 'border-databricks-error focus:ring-databricks-error focus:border-databricks-error' 
            : 'border-databricks-gray-300 focus:ring-databricks-blue focus:border-databricks-blue'
          }
          focus:outline-none focus:ring-2 focus:ring-offset-0
          disabled:bg-databricks-gray-100 disabled:cursor-not-allowed
          ${className}`}
        style={{
          backgroundImage: `url("data:image/svg+xml,%3csvg xmlns='http://www.w3.org/2000/svg' fill='none' viewBox='0 0 20 20'%3e%3cpath stroke='%236B6B6B' stroke-linecap='round' stroke-linejoin='round' stroke-width='1.5' d='M6 8l4 4 4-4'/%3e%3c/svg%3e")`,
          backgroundPosition: 'right 0.5rem center',
          backgroundSize: '1.5em 1.5em',
        }}
        {...props}
      >
        {options.map((option) => (
          <option key={option.value} value={option.value}>
            {option.label}
          </option>
        ))}
      </select>
      {error && (
        <p className="mt-1 text-sm text-databricks-error">{error}</p>
      )}
      {helperText && !error && (
        <p className="mt-1 text-sm text-databricks-gray-500">{helperText}</p>
      )}
    </div>
  )
}
